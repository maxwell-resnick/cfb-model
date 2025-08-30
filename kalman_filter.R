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
      regexpr("^([A-Za-z0-9_:.]+) (season_num|team_game_num|opp_game_num)\\(", Ttxt)
    )
    groups <- if (length(m)) unique(sub(" (season_num|team_game_num|opp_game_num)\\(.*$", "", m)) else character(0)
  }
  if (!length(groups)) return(list())
  
  last_number <- function(line) {
    xs <- strsplit(line, " ", fixed = TRUE)[[1]]
    suppressWarnings(as.numeric(tail(xs, 1)))
  }
  first_line_idx <- function(g) {
    hit <- grep(paste0("^", g, " (season_num|team_game_num|opp_game_num)\\("), Ttxt, perl = TRUE)
    if (!length(hit)) return(NA_integer_)
    hit[1]
  }
  
  out <- setNames(vector("list", length(groups)), groups)
  for (g in groups) {
    i1 <- first_line_idx(g)
    if (is.na(i1)) next
    sdv <- last_number(Ttxt[i1])
    j <- NA_integer_
    for (k in (i1 + 1):length(Ttxt)) {
      if (grepl("^(season_num|team_game_num|opp_game_num)\\(", Ttxt[k])) { j <- k; break }
      if (grepl("^Groups\\s+\\S+", Ttxt[k]) || grepl("^\\S+\\s+(season_num|team_game_num|opp_game_num)\\(", Ttxt[k])) break
    }
    if (is.na(j)) next
    phi <- last_number(Ttxt[j])
    if (is.na(sdv) || is.na(phi)) next
    tau2 <- sdv^2
    q    <- tau2 * (1 - phi^2)
    out[[g]] <- list(tau2 = tau2, phi = phi, q = q)
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

kf_update_diag <- function(resid, m_vec, P_vec, sigma_e2) {
  S     <- sum(P_vec) + sigma_e2
  innov <- resid - sum(m_vec)
  K     <- P_vec / S
  m_new <- m_vec + K * innov
  P_new <- P_vec - (P_vec * P_vec) / S
  list(m = m_new, P = P_new, K = K, S = S, innov = innov)
}

# ============================================================
# Kalman forward filter for OU states over:
#   - season (team_f, opponent_f)
#   - game-number within season (team_f:season, opponent_f:season)
# If y is NA → prediction only (no update). No static intercepts. No HFA.
# ============================================================
kalman_forward_ou <- function(
    fit, initial_train_data, future_df,
    last_train_season
) {
  outcome_col <- .response_var(fit)
  sigma_e2    <- sigma(fit)^2
  
  ou <- extract_ou_from_summary(fit, groups = NULL)
  has_season_team <- "team_f"            %in% names(ou)
  has_season_opp  <- "opponent_f"        %in% names(ou)
  has_game_team   <- "team_f:season"     %in% names(ou)
  has_game_opp    <- "opponent_f:season" %in% names(ou)
  
  # Seeds for season OU
  if (has_season_team) seed_off <- extract_last_season_blups(fit, "team_f",     last_train_season)
  if (has_season_opp)  seed_def <- extract_last_season_blups(fit, "opponent_f", last_train_season)
  
  # Storage
  season_off_tbl <- tibble(team = character(), season = integer(), m = numeric(), P = numeric())
  season_def_tbl <- tibble(team = character(), season = integer(), m = numeric(), P = numeric())
  # per (team, season) environments for game-number OU, tracking last index
  game_off_env <- if (has_game_team) new.env(hash = TRUE, parent = emptyenv()) else NULL
  game_def_env <- if (has_game_opp)  new.env(hash = TRUE, parent = emptyenv()) else NULL
  
  # Keep strictly > last_train_season, sorted by time
  df <- future_df %>%
    filter(season_num >= last_train_season + 1L) %>%
    arrange(season_num, start_dt, team)
  
  if (!nrow(df)) stop("No rows strictly after training season (", last_train_season, ").")
  
  # Fixed effects
  fx <- fixed_components(fit, df)
  y  <- df[[outcome_col]]  # NA for unplayed
  
  kstep_predict <- function(prev_m, prev_P, phi, tau2, k_steps) {
    if (!is.finite(prev_m) || is.na(prev_m)) {
      list(m = 0, P = tau2)  # first observation for that team-season
    } else {
      if (k_steps <= 0) k_steps <- 1L
      phi_k <- phi^k_steps
      m_k <- phi_k * prev_m
      P_k <- (phi^(2*k_steps)) * prev_P + tau2 * (1 - phi^(2*k_steps))
      list(m = m_k, P = P_k)
    }
  }
  
  game_log <- vector("list", nrow(df))
  
  seasons <- sort(unique(df$season_num))
  for (s in seasons) {
    dS <- filter(df, season_num == s)
    teams_s <- sort(unique(c(as.character(dS$team_f), as.character(dS$opponent_f))))
    
    # Season-level priors (offense)
    if (has_season_team) {
      if (s == last_train_season + 1L) {
        m_prev <- seed_off$m[match(teams_s, seed_off$team)]
      } else {
        prev_off <- filter(season_off_tbl, season == (s - 1L))
        m_prev <- prev_off$m[match(teams_s, prev_off$team)]
      }
      m0 <- ou[["team_f"]]$phi * m_prev
      P0 <- ou[["team_f"]]$tau2
      st_off <- tibble(team = teams_s, season = s,
                       m = replace(m0, is.na(m0), 0),
                       P = P0)
      season_off_tbl <- bind_rows(season_off_tbl, st_off)
    }
    
    # Season-level priors (defense)
    if (has_season_opp) {
      if (s == last_train_season + 1L) {
        m_prev <- seed_def$m[match(teams_s, seed_def$team)]
      } else {
        prev_def <- filter(season_def_tbl, season == (s - 1L))
        m_prev <- prev_def$m[match(teams_s, prev_def$team)]
      }
      m0 <- ou[["opponent_f"]]$phi * m_prev
      P0 <- ou[["opponent_f"]]$tau2
      st_def <- tibble(team = teams_s, season = s,
                       m = replace(m0, is.na(m0), 0),
                       P = P0)
      season_def_tbl <- bind_rows(season_def_tbl, st_def)
    }
    
    # Reset per-season game-number trackers
    if (has_game_team) rm(list = ls(game_off_env), envir = game_off_env)
    if (has_game_opp)  rm(list = ls(game_def_env), envir = game_def_env)
    
    # Iterate games in chronological order within the season
    idx_s <- which(df$season_num == s)
    for (i in idx_s) {
      team <- as.character(df$team_f[i])
      opp  <- as.character(df$opponent_f[i])
      
      # ---- Season OU states (already priored) ----
      m_so <- if (has_season_team) season_off_tbl$m[which(season_off_tbl$team == team & season_off_tbl$season == s)] else NULL
      P_so <- if (has_season_team) season_off_tbl$P[which(season_off_tbl$team == team & season_off_tbl$season == s)] else NULL
      m_sd <- if (has_season_opp)  season_def_tbl$m[which(season_def_tbl$team == opp  & season_def_tbl$season == s)]  else NULL
      P_sd <- if (has_season_opp)  season_def_tbl$P[which(season_def_tbl$team == opp  & season_def_tbl$season == s)]  else NULL
      
      # ---- Game-number OU states (team side) ----
      if (has_game_team) {
        key_team <- paste(team, s, sep = "||")
        cur_idx  <- as.integer(df$team_game_number[i])
        if (!exists(key_team, envir = game_off_env, inherits = FALSE)) {
          po <- list(m = NA_real_, P = ou[["team_f:season"]]$tau2, last_idx = NA_integer_)
        } else {
          po <- get(key_team, envir = game_off_env)
        }
        k_steps <- if (is.na(po$last_idx)) 1L else max(1L, cur_idx - po$last_idx)
        pred_o  <- kstep_predict(po$m, po$P, ou[["team_f:season"]]$phi, ou[["team_f:season"]]$tau2, k_steps)
        m_wt <- pred_o$m; P_wt <- pred_o$P
        assign(key_team, list(m = m_wt, P = P_wt, last_idx = cur_idx), envir = game_off_env)
      } else {
        m_wt <- NULL; P_wt <- NULL
      }
      
      # ---- Game-number OU states (opponent side) ----
      if (has_game_opp) {
        key_opp <- paste(opp, s, sep = "||")
        cur_i2  <- as.integer(df$opponent_game_number[i])
        if (!exists(key_opp, envir = game_def_env, inherits = FALSE)) {
          pd <- list(m = NA_real_, P = ou[["opponent_f:season"]]$tau2, last_idx = NA_integer_)
        } else {
          pd <- get(key_opp, envir = game_def_env)
        }
        k_steps <- if (is.na(pd$last_idx)) 1L else max(1L, cur_i2 - pd$last_idx)
        pred_d  <- kstep_predict(pd$m, pd$P, ou[["opponent_f:season"]]$phi, ou[["opponent_f:season"]]$tau2, k_steps)
        m_wo <- pred_d$m; P_wo <- pred_d$P
        assign(key_opp, list(m = m_wo, P = P_wo, last_idx = cur_i2), envir = game_def_env)
      } else {
        m_wo <- NULL; P_wo <- NULL
      }
      
      # Build vectors in fixed order
      m_vec <- c(if (has_season_team) m_so else NULL,
                 if (has_season_opp)  m_sd else NULL,
                 if (has_game_team)   m_wt else NULL,
                 if (has_game_opp)    m_wo else NULL)
      P_vec <- c(if (has_season_team) P_so else NULL,
                 if (has_season_opp)  P_sd else NULL,
                 if (has_game_team)   P_wt else NULL,
                 if (has_game_opp)    P_wo else NULL)
      
      # PRE-GAME effects (prediction-only)
      prior_season_team <- if (has_season_team) m_so else 0
      prior_season_opp  <- if (has_season_opp)  m_sd else 0
      prior_game_team   <- if (has_game_team)   m_wt else 0
      prior_game_opp    <- if (has_game_opp)    m_wo else 0
      
      game_log[[i]] <- tibble(
        season = s,
        # keep original week for convenience if present
        week = suppressWarnings(as.integer(df$week[i])),
        team = team,
        opponent = opp,
        team_talent_effect     = fx$team_talent_effect[i],
        opponent_talent_effect = fx$opponent_talent_effect[i],
        prior_season_team = prior_season_team,
        prior_season_opp  = prior_season_opp,
        prior_week_team   = prior_game_team,  # naming kept for downstream compatibility
        prior_week_opp    = prior_game_opp
      )
      
      # UPDATE if observed
      if (!is.na(y[i]) && length(m_vec)) {
        resid_i <- y[i] - fx$fixed_sum[i]
        up <- kf_update_diag(resid_i, m_vec, P_vec, sigma_e2)
        cursor <- 1L
        if (has_season_team) {
          so_row <- which(season_off_tbl$team == team & season_off_tbl$season == s)
          season_off_tbl$m[so_row] <- up$m[cursor]; season_off_tbl$P[so_row] <- up$P[cursor]; cursor <- cursor + 1L
        }
        if (has_season_opp) {
          sd_row <- which(season_def_tbl$team == opp  & season_def_tbl$season == s)
          season_def_tbl$m[sd_row] <- up$m[cursor]; season_def_tbl$P[sd_row] <- up$P[cursor]; cursor <- cursor + 1L
        }
        if (has_game_team) {
          key_team <- paste(team, s, sep = "||")
          cur <- get(key_team, envir = game_off_env); cur$m <- up$m[cursor]; cur$P <- up$P[cursor]
          assign(key_team, cur, envir = game_off_env); cursor <- cursor + 1L
        }
        if (has_game_opp) {
          key_opp <- paste(opp, s, sep = "||")
          cur <- get(key_opp, envir = game_def_env); cur$m <- up$m[cursor]; cur$P <- up$P[cursor]
          assign(key_opp, cur, envir = game_def_env); cursor <- cursor + 1L
        }
      }
    } # end loop season i
  }   # end seasons
  
  fx_tbl <- bind_rows(game_log) %>%
    transmute(
      season, week, team, opponent,
      offense_talent_effect = team_talent_effect,
      offense_season_effect = prior_season_team,
      offense_week_effect   = prior_week_team,  # "week" label retained
      offense_effect        = offense_talent_effect + offense_season_effect + offense_week_effect,
      defense_talent_effect = opponent_talent_effect,
      defense_season_effect = prior_season_opp,
      defense_week_effect   = prior_week_opp,
      defense_effect        = defense_talent_effect + defense_season_effect + defense_week_effect
    )
  list(pregame = fx_tbl,
       season_off = season_off_tbl,
       season_def = season_def_tbl)
}

run_pipeline <- function(fit, initial_train_data, future_df_aligned, last_train_season, label) {
  res <- kalman_forward_ou(
    fit                 = fit,
    initial_train_data  = initial_train_data,
    future_df           = future_df_aligned,
    last_train_season   = last_train_season
  )
  res$pregame %>%
    mutate(model = label) %>%
    select(model, season, week, team, opponent, offense_effect, defense_effect)
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