# ---- Packages ----
library(glmmTMB)
library(dplyr)
library(tidyr)
library(tibble)
library(DBI)
library(RPostgres)
library(qs)
library(lubridate)

# ============================================================
# Helpers: recover levels from saved models & align future data
# ============================================================
get_levels <- function(fit) {
  mf <- tryCatch(model.frame(fit), error = function(e) NULL)
  if (!is.null(mf)) {
    levs <- c(
      if ("team_f" %in% names(mf))     levels(mf$team_f)     else character(),
      if ("opponent_f" %in% names(mf)) levels(mf$opponent_f) else character()
    )
    levs <- unique(levs[!is.na(levs)])
    if (length(levs)) return(sort(levs))
  }
  re <- tryCatch(ranef(fit)$cond, error = function(e) NULL)
  if (!is.null(re)) {
    levs <- unlist(lapply(re[c("team_f","opponent_f")], function(tab) {
      if (is.null(tab)) return(character())
      rn <- rownames(as.data.frame(tab))
      sub(":.*$", "", rn)
    }))
    levs <- unique(levs[!is.na(levs)])
    if (length(levs)) return(sort(levs))
  }
  stop("Could not recover factor levels from model.")
}

align_future_levels <- function(future_df, fits) {
  model_levels <- unique(sort(unlist(lapply(fits, get_levels))))
  future_teams <- unique(sort(c(as.character(future_df$team), as.character(future_df$opponent))))
  all_levels   <- unique(c(model_levels, future_teams))
  future_df %>%
    mutate(
      team_f     = factor(as.character(team),     levels = all_levels),
      opponent_f = factor(as.character(opponent), levels = all_levels),
      season_num = as.integer(season)
    )
}

.response_var <- function(fit) as.character(stats::formula(fit)[[2]])

# ============================================================
# Parse OU blocks (phi, tau^2 → q = tau^2(1-phi^2)) from summary
# Works with season_num, team_game_num, opp_game_num
# ============================================================
extract_ou_from_summary <- function(fit, groups = NULL) {
  Ttxt <- gsub("[[:space:]]+", " ", trimws(capture.output(summary(fit))))
  if (is.null(groups)) {
    m <- regmatches(
      Ttxt,
      regexpr("^([A-Za-z0-9_:.]+) (season_num|week_num|team_game_num|opp_game_num)\\(", Ttxt)
    )
    groups <- if (length(m)) unique(sub(" (season_num|week_num|team_game_num|opp_game_num)\\(.*$", "", m)) else character(0)
  }
  if (!length(groups)) return(list())
  
  last_number <- function(line) {
    xs <- strsplit(line, " ", fixed = TRUE)[[1]]
    suppressWarnings(as.numeric(tail(xs, 1)))
  }
  first_line_idx <- function(g) {
    hit <- grep(paste0("^", g, " (season_num|week_num|team_game_num|opp_game_num)\\("), Ttxt, perl = TRUE)
    if (!length(hit)) return(NA_integer_)
    hit[1]
  }
  
  out <- setNames(vector("list", length(groups)), groups)
  for (g in groups) {
    i1 <- first_line_idx(g); if (is.na(i1)) next
    sdv <- last_number(Ttxt[i1])
    j <- NA_integer_
    for (k in (i1 + 1):length(Ttxt)) {
      if (grepl("^(season_num|week_num|team_game_num|opp_game_num)\\(", Ttxt[k])) { j <- k; break }
      if (grepl("^Groups\\s+\\S+", Ttxt[k]) || grepl("^\\S+\\s+(season_num|week_num|team_game_num|opp_game_num)\\(", Ttxt[k])) break
    }
    if (is.na(j)) next
    phi <- last_number(Ttxt[j])
    if (is.na(sdv) || is.na(phi)) next
    tau2 <- sdv^2
    out[[g]] <- list(tau2 = tau2, phi = phi, q = tau2 * (1 - phi^2))
  }
  out[!vapply(out, is.null, logical(1))]
}

# ============================================================
# Seeds from last training season for season OU states
# ============================================================
extract_last_season_blups <- function(fit, group, last_season_year) {
  try_get <- function(x) tryCatch(x, error = function(e) NULL)
  col_y   <- paste0("season_num(", last_season_year, ")")
  grab <- function(tab) {
    if (is.null(tab)) return(NULL)
    d <- as.data.frame(tab)
    if (!any(names(d) == "..rowname..")) d <- tibble::rownames_to_column(d, "..rowname..")
    team_col <- NULL
    for (cand in c("grp","level.1","group","condGrp")) if (cand %in% names(d)) { team_col <- cand; break }
    if (is.null(team_col)) {
      d$..team_from_rn.. <- gsub(":?season_num\\([^)]*\\)|:?team_game_num\\([^)]*\\)|:?opp_game_num\\([^)]*\\)", "", d[["..rowname.."]])
      team_col <- "..team_from_rn.."
    }
    if (col_y %in% names(d)) {
      out <- tibble(
        team = as.character(d[[team_col]]),
        m    = as.numeric(d[[col_y]]),
        P    = NA_real_
      )
      out$team <- sub(":?season_num\\([^)]*\\)$", "", out$team)
      out <- out[!duplicated(out$team, fromLast = TRUE), , drop = FALSE]
      rownames(out) <- NULL
      return(out)
    }
    NULL
  }
  out <- grab(try_get(ranef(fit, condVar = TRUE)$cond[[group]]))
  if (!is.null(out)) return(out)
  out <- grab(try_get(coef(fit)$cond[[group]]))
  if (!is.null(out)) return(out)
  stop("Could not extract season BLUP seeds for group '", group,
       "' at season ", last_season_year, ".")
}

# ============================================================
# Fixed effects WITHOUT home-field: only talent terms
# (adds any missing fixed columns as zeros so HFA contributes 0)
# ============================================================
fixed_components <- function(fit, new_df) {
  beta <- fixef(fit)$cond
  if (is.null(beta) || length(beta) == 0) {
    n <- nrow(new_df)
    return(list(
      team_talent_effect     = rep(0, n),
      opponent_talent_effect = rep(0, n),
      fixed_sum              = rep(0, n)
    ))
  }
  cols_needed <- unique(setdiff(names(beta), "(Intercept)"))
  for (nm in cols_needed) {
    if (!nm %in% names(new_df)) new_df[[nm]] <- 0
    new_df[[nm]] <- dplyr::coalesce(as.numeric(new_df[[nm]]), 0)
  }
  ff <- reformulate(termlabels = cols_needed, intercept = FALSE)
  X  <- model.matrix(ff, data = new_df)
  miss <- setdiff(cols_needed, colnames(X))
  if (length(miss)) {
    X <- cbind(X, matrix(0, nrow(new_df), length(miss), dimnames = list(NULL, miss)))
  }
  keep <- intersect(names(beta), colnames(X))
  X <- X[, keep, drop = FALSE]
  beta_use <- beta[keep]
  comp <- as.numeric(X %*% beta_use)
  get_comp <- function(nm) if (nm %in% colnames(X)) as.numeric(X[, nm] * beta_use[[nm]]) else rep(0, nrow(X))
  list(
    team_talent_effect     = get_comp("team_talent_scaled"),
    opponent_talent_effect = get_comp("opponent_talent_scaled"),
    fixed_sum              = comp
  )
}

kf_update_diag_glmm <- function(resid, m_vec, P_vec, sigma_e2) {
  S     <- sum(P_vec) + sigma_e2
  innov <- resid - sum(m_vec)
  K_i   <- P_vec / S
  m_new <- m_vec + K_i * innov
  P_new <- P_vec - (P_vec * P_vec) / S
  list(m = m_new, P = P_new, K = K_i, S = S, innov = innov)
}

kstep_predict_ou <- function(m_prev, P_prev, phi, tau2, k_steps) {
  if (!is.finite(m_prev) || is.na(m_prev)) return(list(m = 0, P = tau2))
  if (k_steps <= 0) k_steps <- 1L
  phi2k <- phi^(2 * k_steps)
  list(m = (phi^k_steps) * m_prev,
       P = phi2k * P_prev + tau2 * (1 - phi2k))
}

# seeds with posterior variance from last training season
extract_last_season_posterior <- function(fit, group, last_season_year, default_tau2) {
  col_y <- paste0("season_num(", last_season_year, ")")
  safe <- function(x) tryCatch(x, error = function(e) NULL)
  re_obj <- safe(ranef(fit, condVar = TRUE)$cond[[group]])
  
  if (is.null(re_obj)) {
    cf <- safe(coef(fit)$cond[[group]])
    if (!is.null(cf) && col_y %in% names(cf)) {
      df <- tibble::rownames_to_column(as.data.frame(cf), "grp")
      return(tibble::tibble(team = sub(":.*$", "", df$grp),
                            m = suppressWarnings(as.numeric(df[[col_y]])),
                            P = rep(default_tau2, nrow(df))))
    }
    stop("Could not extract last-season posterior for group '", group, "'.")
  }
  
  df <- as.data.frame(re_obj)
  if (!any(names(df) == "..rowname..")) df <- tibble::rownames_to_column(df, "..rowname..")
  teams <- sub(":.*$", "", df[["..rowname.."]])
  means <- suppressWarnings(as.numeric(df[[col_y]]))
  
  Pvec <- {
    PV <- attr(re_obj, "postVar")
    if (!is.null(PV) && length(dim(PV)) == 3L) {
      cols <- setdiff(colnames(df), "..rowname..")
      j <- match(col_y, cols)
      if (!is.na(j)) PV[j, j, ] else rep(default_tau2, length(means))
    } else rep(default_tau2, length(means))
  }
  tibble::tibble(team = teams, m = means, P = Pvec)
}

kalman_forward_ou <- function(
    fit,
    future_df,            # needs: season_num, week, start_dt, team_f, opponent_f, played
    last_train_season,
    obs_var_mult = 1.0
) {
  outcome_col <- .response_var(fit)
  sigma_e2    <- obs_var_mult * sigma(fit)^2
  ou          <- extract_ou_from_summary(fit, groups = NULL)
  
  has_season_team <- "team_f"            %in% names(ou)
  has_season_opp  <- "opponent_f"        %in% names(ou)
  has_week_team   <- "team_f:season"     %in% names(ou)
  has_week_opp    <- "opponent_f:season" %in% names(ou)
  
  # seeds (means; variance handled via tau2 fallback here)
  if (has_season_team) seed_off <- extract_last_season_blups(fit, "team_f",     last_train_season)
  if (has_season_opp)  seed_def <- extract_last_season_blups(fit, "opponent_f", last_train_season)
  
  df <- future_df %>%
    dplyr::filter(season_num >= last_train_season + 1L) %>%
    dplyr::arrange(season_num, start_dt, team_f) %>%
    dplyr::mutate(played = as.logical(played))
  
  if (!nrow(df)) stop("No rows strictly after training season (", last_train_season, ").")
  
  fx <- fixed_components(fit, df)
  
  # IMPORTANT: do not update from unplayed rows
  y <- df[[outcome_col]]
  y[!df$played] <- NA_real_
  
  kstep_predict <- function(prev_m, prev_P, phi, tau2, k_steps) {
    if (!is.finite(prev_m) || is.na(prev_m)) return(list(m = 0, P = tau2))
    if (k_steps <= 0) k_steps <- 1L
    phi2k <- phi^(2 * k_steps)
    list(m = (phi^k_steps) * prev_m,
         P = phi2k * prev_P + tau2 * (1 - phi2k))
  }
  
  season_off_tbl <- tibble::tibble(team = character(), season = integer(), m = numeric(), P = numeric())
  season_def_tbl <- tibble::tibble(team = character(), season = integer(), m = numeric(), P = numeric())
  week_off_env <- if (has_week_team) new.env(hash = TRUE, parent = emptyenv()) else NULL
  week_def_env <- if (has_week_opp)  new.env(hash = TRUE, parent = emptyenv()) else NULL
  
  game_log <- vector("list", nrow(df))
  
  seasons <- sort(unique(df$season_num))
  for (s in seasons) {
    dS <- dplyr::filter(df, season_num == s)
    teams_s <- sort(unique(c(as.character(dS$team_f), as.character(dS$opponent_f))))
    
    if (has_season_team) {
      m_prev <- if (s == last_train_season + 1L)
        seed_off$m[match(teams_s, seed_off$team)]
      else dplyr::filter(season_off_tbl, season == s - 1L)$m[match(teams_s, dplyr::filter(season_off_tbl, season == s - 1L)$team)]
      season_off_tbl <- dplyr::bind_rows(season_off_tbl,
                                         tibble::tibble(team = teams_s, season = s,
                                                        m = dplyr::coalesce(ou[["team_f"]]$phi * m_prev, 0),
                                                        P = ou[["team_f"]]$tau2))
    }
    if (has_season_opp) {
      m_prev <- if (s == last_train_season + 1L)
        seed_def$m[match(teams_s, seed_def$team)]
      else dplyr::filter(season_def_tbl, season == s - 1L)$m[match(teams_s, dplyr::filter(season_def_tbl, season == s - 1L)$team)]
      season_def_tbl <- dplyr::bind_rows(season_def_tbl,
                                         tibble::tibble(team = teams_s, season = s,
                                                        m = dplyr::coalesce(ou[["opponent_f"]]$phi * m_prev, 0),
                                                        P = ou[["opponent_f"]]$tau2))
    }
    
    if (has_week_team) rm(list = ls(week_off_env), envir = week_off_env)
    if (has_week_opp)  rm(list = ls(week_def_env), envir = week_def_env)
    
    idx_s <- which(df$season_num == s)
    for (i in idx_s) {
      team <- as.character(df$team_f[i])
      opp  <- as.character(df$opponent_f[i])
      wk_i <- suppressWarnings(as.integer(df$week[i]))
      
      # season states (current)
      m_so <- if (has_season_team) season_off_tbl$m[which(season_off_tbl$team == team & season_off_tbl$season == s)] else NULL
      P_so <- if (has_season_team) season_off_tbl$P[which(season_off_tbl$team == team & season_off_tbl$season == s)] else NULL
      m_sd <- if (has_season_opp)  season_def_tbl$m[which(season_def_tbl$team == opp  & season_def_tbl$season == s)]  else NULL
      P_sd <- if (has_season_opp)  season_def_tbl$P[which(season_def_tbl$team == opp  & season_def_tbl$season == s)]  else NULL
      
      # week OU (offense)
      if (has_week_team) {
        key_off <- paste(team, s, sep = "||")
        if (!exists(key_off, envir = week_off_env, inherits = FALSE)) {
          po <- list(m = NA_real_, P = ou[["team_f:season"]]$tau2, last_wk = NA_integer_)
        } else po <- get(key_off, envir = week_off_env)
        k_steps <- if (is.na(po$last_wk)) 1L else max(1L, wk_i - po$last_wk)
        pred_o  <- kstep_predict(po$m, po$P, ou[["team_f:season"]]$phi, ou[["team_f:season"]]$tau2, k_steps)
        m_wt <- pred_o$m; P_wt <- pred_o$P
        assign(key_off, list(m = m_wt, P = P_wt, last_wk = wk_i), envir = week_off_env)
      } else { m_wt <- NULL; P_wt <- NULL }
      
      # week OU (defense)
      if (has_week_opp) {
        key_def <- paste(opp, s, sep = "||")
        if (!exists(key_def, envir = week_def_env, inherits = FALSE)) {
          pd <- list(m = NA_real_, P = ou[["opponent_f:season"]]$tau2, last_wk = NA_integer_)
        } else pd <- get(key_def, envir = week_def_env)
        k_steps_def <- if (is.na(pd$last_wk)) 1L else max(1L, wk_i - pd$last_wk)
        pred_d  <- kstep_predict(pd$m, pd$P, ou[["opponent_f:season"]]$phi, ou[["opponent_f:season"]]$tau2, k_steps_def)
        m_wo <- pred_d$m; P_wo <- pred_d$P
        assign(key_def, list(m = m_wo, P = P_wo, last_wk = wk_i), envir = week_def_env)
      } else { m_wo <- NULL; P_wo <- NULL }
      
      # build state vectors (+1 loadings, matching glmmTMB)
      m_vec <- c(if (has_season_team) m_so else NULL,
                 if (has_season_opp)  m_sd else NULL,
                 if (has_week_team)   m_wt else NULL,
                 if (has_week_opp)    m_wo else NULL)
      P_vec <- c(if (has_season_team) P_so else NULL,
                 if (has_season_opp)  P_sd else NULL,
                 if (has_week_team)   P_wt else NULL,
                 if (has_week_opp)    P_wo else NULL)
      
      # ---- PRE-GAME LOG (adds expected/observed opponent output) ----
      state_sum <- sum(m_vec)
      exp_y     <- fx$fixed_sum[i] + state_sum
      obs_y     <- y[i]  # already NA for unplayed
      game_log[[i]] <- tibble::tibble(
        season = s,
        week = wk_i,
        team = team,
        opponent = opp,
        team_talent_effect     = fx$team_talent_effect[i],
        opponent_talent_effect = fx$opponent_talent_effect[i],
        prior_season_team = if (length(m_so)) m_so else 0,
        prior_season_opp  = if (length(m_sd)) m_sd else 0,
        prior_week_team   = if (length(m_wt)) m_wt else 0,
        prior_week_opp    = if (length(m_wo)) m_wo else 0,
        expected_opponent_output = exp_y,
        observed_opponent_output = obs_y
      )
      
      # ---- POSTERIOR UPDATE (only when played) ----
      if (!is.na(obs_y) && length(m_vec)) {
        up <- kf_update_diag_glmm(obs_y - fx$fixed_sum[i], m_vec, P_vec, sigma_e2)
        cursor <- 1L
        if (has_season_team) { so_row <- which(season_off_tbl$team == team & season_off_tbl$season == s)
        season_off_tbl$m[so_row] <- up$m[cursor]; season_off_tbl$P[so_row] <- up$P[cursor]; cursor <- cursor + 1L }
        if (has_season_opp)  { sd_row <- which(season_def_tbl$team == opp  & season_def_tbl$season == s)
        season_def_tbl$m[sd_row] <- up$m[cursor]; season_def_tbl$P[sd_row] <- up$P[cursor]; cursor <- cursor + 1L }
        if (has_week_team)   { key_off <- paste(team, s, sep = "||")
        cur <- get(key_off, envir = week_off_env); cur$m <- up$m[cursor]; cur$P <- up$P[cursor]
        assign(key_off, cur, envir = week_off_env); cursor <- cursor + 1L }
        if (has_week_opp)    { key_def <- paste(opp, s, sep = "||")
        cur <- get(key_def, envir = week_def_env); cur$m <- up$m[cursor]; cur$P <- up$P[cursor]
        assign(key_def, cur, envir = week_def_env); cursor <- cursor + 1L }
      }
    }
  }
  
  game_log_df <- dplyr::bind_rows(Filter(Negate(is.null), game_log))
  if (is.null(game_log_df) || nrow(game_log_df) == 0L) {
    return(list(
      pregame = tibble::tibble(season=integer(), week=integer(), team=character(), opponent=character(),
                               offense_effect=numeric(), defense_effect=numeric(),
                               expected_opponent_output=numeric(), observed_opponent_output=numeric()),
      season_off = season_off_tbl, season_def = season_def_tbl))
  }
  
  fx_tbl <- game_log_df %>%
    dplyr::mutate(
      offense_random = prior_season_team + prior_week_team,
      defense_random = prior_season_opp  + prior_week_opp,     # good defense => negative
      offense_effect = team_talent_effect + offense_random,     # (no HFA here)
      defense_effect = opponent_talent_effect + defense_random  # (no HFA here)
    ) %>%
    dplyr::select(season, week, team, opponent,
                  offense_effect, defense_effect,
                  expected_opponent_output, observed_opponent_output)
  
  list(pregame = fx_tbl, season_off = season_off_tbl, season_def = season_def_tbl)
}


run_pipeline <- function(fit, initial_train_data, future_df_aligned, last_train_season, label) {
  res <- kalman_forward_ou(
    fit                 = fit,
    future_df           = future_df_aligned,
    last_train_season   = last_train_season
  )
  res$pregame %>%
    dplyr::mutate(model = label) %>%
    dplyr::select(model, season, week, team, opponent,
                  offense_effect, defense_effect,
                  expected_opponent_output, observed_opponent_output)
}

get_pregame_latents <- function() {
  fit_overall <- qread("glmms/fit_overall.qs")
  fit_passing <- qread("glmms/fit_passing.qs")
  fit_rushing <- qread("glmms/fit_rushing.qs")
  # ... (your data loading / future_df_aligned building stays the same)
  
  pregame_overall <- run_pipeline(fit_overall, model_df, future_df_aligned, 2020, "overall")
  pregame_passing <- run_pipeline(fit_passing, model_df, future_df_aligned, 2020, "passing")
  pregame_rushing <- run_pipeline(fit_rushing, model_df, future_df_aligned, 2020, "rushing")
  
  pregame_latents_all <- dplyr::bind_rows(pregame_overall, pregame_passing, pregame_rushing)
  
  pregame_latents_all %>%
    dplyr::distinct(season, week, team, opponent, model,
                    offense_effect, defense_effect,
                    expected_opponent_output, observed_opponent_output) %>%
    tidyr::pivot_wider(
      id_cols    = c(season, week, team, opponent),
      names_from = model,
      values_from = c(offense_effect, defense_effect,
                      expected_opponent_output, observed_opponent_output),
      names_glue = "{.value}_{model}"
    ) %>%
    dplyr::arrange(season, week, team, opponent)
}


# ============================
# ==== EXECUTION PIPELINE  ====
# ============================

get_pregame_latents <- function() {
  # 1) Load saved models (trained through 2020; OU over game-number)
  fit_overall <- qread("glmms/fit_overall.qs")
  fit_passing <- qread("glmms/fit_passing.qs")
  fit_rushing <- qread("glmms/fit_rushing.qs")
  
  connect_neon <- function() {
    library(DBI); library(RPostgres); library(httr)
    
    url <- Sys.getenv("DATABASE_URL", unset = "")
    if (nzchar(url)) {
      pu <- httr::parse_url(url)
      host <- pu$hostname
      port <- as.integer(pu$port %||% 5432L)
      db   <- sub("^/", "", pu$path %||% "")
      user <- utils::URLdecode(pu$username %||% "")
      pass <- utils::URLdecode(pu$password %||% "")
      ssl  <- pu$query$sslmode %||% "require"
    } else {
      host <- Sys.getenv("PGHOST")
      port <- as.integer(Sys.getenv("PGPORT", "5432"))
      db   <- Sys.getenv("PGDATABASE")
      user <- Sys.getenv("PGUSER")
      pass <- Sys.getenv("PGPASSWORD")
      ssl  <- Sys.getenv("PGSSLMODE", "require")
    }
    
    stopifnot(nzchar(host), nzchar(db), nzchar(user), nzchar(pass))
    DBI::dbConnect(
      RPostgres::Postgres(),
      host     = host,
      port     = port,
      dbname   = db,
      user     = user,
      password = pass,
      sslmode  = ssl
    )
  }
  
  con <- connect_neon()
  
  model_df <- dbGetQuery(con, 'SELECT * FROM "PreparedData" WHERE season <= 2020;')
  
  # 3) Pull future rows 2021–2025 (played + unplayed)
  future_df_raw <- dbGetQuery(
    con,
    'SELECT * FROM "PreparedData" WHERE season BETWEEN 2021 AND 2025;'
  )
  dbDisconnect(con)
  
  # --- Build team/opponent game numbers within season ---
  future_df_raw <- future_df_raw %>%
    mutate(start_dt = suppressWarnings(lubridate::ymd_hms(startDate, quiet = TRUE))) %>%
    arrange(team, season, start_dt, .by_group = FALSE) %>%
    group_by(team, season) %>%
    mutate(team_game_number = row_number()) %>%
    ungroup() %>%
    arrange(opponent, season, start_dt, .by_group = FALSE) %>%
    group_by(opponent, season) %>%
    mutate(opponent_game_number = row_number()) %>%
    ungroup()
  
  # 4) Weighted imputation for 2025 talent
  weights_tbl <- tibble::tibble(
    season = c(2024L, 2023L, 2022L),
    w      = c(0.6,    0.3,    0.1)
  )
  hist_talent <- future_df_raw %>%
    filter(season %in% weights_tbl$season) %>%
    select(season, team, team_talent) %>%
    mutate(team_talent = suppressWarnings(as.numeric(team_talent))) %>%
    group_by(team, season) %>%
    summarise(talent = median(team_talent, na.rm = TRUE), .groups = "drop") %>%
    left_join(weights_tbl, by = "season") %>%
    group_by(team) %>%
    summarise(
      imputed_talent_2025 = if (all(is.na(talent))) NA_real_
      else sum(w * talent, na.rm = TRUE) / sum(w[!is.na(talent)]),
      .groups = "drop"
    )
  team_imp <- hist_talent %>% rename(team = team,     team_talent_2025_imp = imputed_talent_2025)
  opp_imp  <- hist_talent %>% rename(opponent = team, opponent_talent_2025_imp = imputed_talent_2025)
  
  # 5) Build future_df
  future_df <- future_df_raw %>%
    mutate(
      season = as.integer(season),
      week   = suppressWarnings(as.integer(week)),
      played = !is.na(homePoints) & !is.na(awayPoints),
      is_home = dplyr::case_when(
        !is.na(homeTeam) & !is.na(team) & homeTeam == team ~ 1L,
        !is.na(awayTeam) & !is.na(team) & awayTeam == team ~ 0L,
        TRUE ~ NA_integer_
      ),
      points_above_average           = 65 * as.numeric(offense_ppa),
      `passing_points_above_average` = 65 * as.numeric(`offense_passingPlays.ppa`),
      `rushing_points_above_average` = 65 * as.numeric(`offense_rushingPlays.ppa`),
      team_talent     = suppressWarnings(as.numeric(team_talent)),
      opponent_talent = suppressWarnings(as.numeric(opponent_talent))
    ) %>%
    left_join(team_imp, by = "team") %>%
    left_join(opp_imp,  by = "opponent") %>%
    mutate(
      team_talent     = if_else(season == 2025 & is.na(team_talent),     team_talent_2025_imp,     team_talent),
      opponent_talent = if_else(season == 2025 & is.na(opponent_talent), opponent_talent_2025_imp, opponent_talent)
    ) %>%
    group_by(season) %>%
    mutate(
      team_talent_scaled     = as.numeric(scale(team_talent)),
      opponent_talent_scaled = as.numeric(scale(opponent_talent))
    ) %>%
    ungroup() %>%
    mutate(
      team_talent_scaled     = if_else(season == 2025 & is.na(team_talent_scaled), 0, team_talent_scaled),
      opponent_talent_scaled = if_else(season == 2025 & is.na(opponent_talent_scaled), 0, opponent_talent_scaled)
    ) %>%
    select(-team_talent_2025_imp, -opponent_talent_2025_imp)
  
  # 6) Align levels to the models
  future_df_aligned <- align_future_levels(
    future_df,
    fits = list(fit_overall, fit_passing, fit_rushing)
  ) %>%
    mutate(
      start_dt = suppressWarnings(lubridate::ymd_hms(startDate, quiet = TRUE)),
      team_game_number     = as.integer(team_game_number),
      opponent_game_number = as.integer(opponent_game_number)
    )
  
  # 7) Run mixed-observation forward filter
  pregame_overall <- run_pipeline(
    fit = fit_overall, initial_train_data = model_df,
    future_df_aligned = future_df_aligned, last_train_season = 2020, label = "overall"
  )
  pregame_passing <- run_pipeline(
    fit = fit_passing, initial_train_data = model_df,
    future_df_aligned = future_df_aligned, last_train_season = 2020, label = "passing"
  )
  pregame_rushing <- run_pipeline(
    fit = fit_rushing, initial_train_data = model_df,
    future_df_aligned = future_df_aligned, last_train_season = 2020, label = "rushing"
  )
  
  pregame_latents_all <- bind_rows(pregame_overall, pregame_passing, pregame_rushing)
  
  pregame_latents <- pregame_latents_all %>%
    distinct(season, week, team, opponent, model, offense_effect, defense_effect) %>%
    pivot_wider(
      id_cols    = c(season, week, team, opponent),
      names_from = model,
      values_from = c(offense_effect, defense_effect),
      names_glue = "{.value}_{model}"
    ) %>%
    arrange(season, week, team, opponent)
  
  return(pregame_latents)
}

