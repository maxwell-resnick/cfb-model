source("kalman_filter.R")

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

xgb_df <- dbGetQuery(con, 'SELECT * FROM "PreparedData" WHERE season between 2020 and 2024;')

pregame_latents <- get_pregame_latents()

xgb_df <- xgb_df %>%
  mutate(
    # robust parse of ISO timestamps; NAs if missing/invalid
    start_dt = suppressWarnings(ymd_hms(startDate, quiet = TRUE))
  ) %>%
  # team-side game index within (team, season)
  arrange(team, season, start_dt, .by_group = FALSE) %>%
  group_by(team, season) %>%
  mutate(team_game_number = row_number()) %>%
  ungroup() %>%
  # opponent-side game index within (opponent, season)
  arrange(opponent, season, start_dt, .by_group = FALSE) %>%
  group_by(opponent, season) %>%
  mutate(opponent_game_number = row_number()) %>%
  ungroup()

# --- Usual modeling transforms ---
xgb_df <- xgb_df %>%
  mutate(
    season_num = numFactor(season),
    # use game-number indices instead of week
    team_game_num = numFactor(team_game_number),
    opp_game_num  = numFactor(opponent_game_number),
    
    team_f     = factor(team),
    opponent_f = factor(opponent),
    
    passing_plays = offense_passingPlays.totalPPA / offense_passingPlays.ppa,
    rushing_plays = offense_rushingPlays.totalPPA / offense_rushingPlays.ppa,
    total_plays = passing_plays + rushing_plays,
    passing_rate  = passing_plays / (passing_plays + rushing_plays),
    
    points_above_average          = offense_ppa * 65,
    passing_points_above_average  = offense_passingPlays.ppa * 65,
    rushing_points_above_average  = offense_rushingPlays.ppa * 65,
    
    is_home = ifelse(homeTeam == team, 1, 0)
  ) %>%
  group_by(season) %>%
  mutate(
    team_talent_scaled     = as.numeric(scale(team_talent)),
    opponent_talent_scaled = as.numeric(scale(opponent_talent))
  ) %>%
  ungroup()

ppa_map <- xgb_df %>%
  transmute(
    season = as.integer(season),
    team   = as.character(team_f),
    percentPPA = suppressWarnings(as.numeric(percentPPA))
  ) %>%
  group_by(season, team) %>%
  summarise(
    percentPPA = dplyr::first(percentPPA[!is.na(percentPPA)]),
    .groups = "drop"
  )


xgb_df <- xgb_df %>%
  mutate(
    # --- Bovada ---
    bovada_formatted_spread = case_when(
      is_home == 1L ~ -bovada_spread,
      is_home == 0L ~  bovada_spread,
      TRUE          ~ NA_real_
    ),
    bovada_formatted_opening_spread = case_when(
      is_home == 1L ~ -bovada_opening_spread,
      is_home == 0L ~  bovada_opening_spread,
      TRUE          ~ NA_real_
    ),
    bovada_formatted_overunder         = bovada_overunder,
    bovada_formatted_opening_overunder = bovada_opening_overunder,
    
    # --- ESPN Bet ---
    espnbet_formatted_spread = case_when(
      is_home == 1L ~ -espnbet_spread,
      is_home == 0L ~  espnbet_spread,
      TRUE          ~ NA_real_
    ),
    espnbet_formatted_opening_spread = case_when(
      is_home == 1L ~ -espnbet_opening_spread,
      is_home == 0L ~  espnbet_opening_spread,
      TRUE          ~ NA_real_
    ),
    espnbet_formatted_overunder         = espnbet_overunder,
    espnbet_formatted_opening_overunder = espnbet_opening_overunder,
    
    # --- DraftKings ---
    draftkings_formatted_spread = case_when(
      is_home == 1L ~ -draftkings_spread,
      is_home == 0L ~  draftkings_spread,
      TRUE          ~ NA_real_
    ),
    draftkings_formatted_opening_spread = case_when(
      is_home == 1L ~ -draftkings_opening_spread,
      is_home == 0L ~  draftkings_opening_spread,
      TRUE          ~ NA_real_
    ),
    draftkings_formatted_overunder         = draftkings_overunder,
    draftkings_formatted_opening_overunder = draftkings_opening_overunder
  )

spread_scores_keys <- xgb_df %>%
  mutate(
    id       = as.integer(id),
    season   = as.integer(season),
    week     = as.integer(if ("week_num" %in% names(.)) week_num else week),
    team     = as.character(team_f),
    opponent = as.character(opponent_f),
    
    # team-centric points using is_home (1 = home, 0 = away)
    team_points = case_when(
      is_home == 1L ~ homePoints,
      is_home == 0L ~ awayPoints,
      TRUE ~ NA_real_
    ),
    opponent_points = case_when(
      is_home == 1L ~ awayPoints,
      is_home == 0L ~ homePoints,
      TRUE ~ NA_real_
    ),
    score_diff = team_points - opponent_points
  ) %>%
  # attach team/opponent percentPPA from the map
  left_join(ppa_map %>% rename(team_percentPPA = percentPPA),
            by = c("season", "team")) %>%
  left_join(ppa_map %>% rename(opponent_percentPPA = percentPPA),
            by = c("season" = "season", "opponent" = "team")) %>%
  select(
    id, season, week, team, opponent,
    
    # Bovada (formatted + raw)
    bovada_formatted_spread, bovada_formatted_opening_spread,
    bovada_formatted_overunder, bovada_formatted_opening_overunder,
    bovada_spread, bovada_opening_spread,
    bovada_overunder, bovada_opening_overunder,
    
    # ESPN Bet (formatted + raw)
    espnbet_formatted_spread, espnbet_formatted_opening_spread,
    espnbet_formatted_overunder, espnbet_formatted_opening_overunder,
    espnbet_spread, espnbet_opening_spread,
    espnbet_overunder, espnbet_opening_overunder,
    
    # DraftKings (formatted + raw)
    draftkings_formatted_spread, draftkings_formatted_opening_spread,
    draftkings_formatted_overunder, draftkings_formatted_opening_overunder,
    draftkings_spread, draftkings_opening_spread,
    draftkings_overunder, draftkings_opening_overunder,
    
    # derived + talents + new percentPPA fields
    team_points, opponent_points, score_diff,
    team_talent_scaled, opponent_talent_scaled,
    team_percentPPA, opponent_percentPPA,
    passing_rate, total_plays, is_home, startDate,
    homePoints, awayPoints
  ) %>%
  distinct(season, week, team, opponent, .keep_all = TRUE)

combined_df <- pregame_latents %>%
  left_join(spread_scores_keys, by = c("season","week","team","opponent"))

avg3 <- function(a, b, c) {
  n <- (!is.na(a)) + (!is.na(b)) + (!is.na(c))
  (coalesce(a, 0) + coalesce(b, 0) + coalesce(c, 0)) / ifelse(n == 0, NA_real_, n)
}

combined_df <- combined_df %>%
  mutate(
    across(
      c(
        bovada_formatted_spread, espnbet_formatted_spread, draftkings_formatted_spread,
        bovada_formatted_opening_spread, espnbet_formatted_opening_spread, draftkings_formatted_opening_spread,
        bovada_formatted_overunder, espnbet_formatted_overunder, draftkings_formatted_overunder,
        bovada_formatted_opening_overunder, espnbet_formatted_opening_overunder, draftkings_formatted_opening_overunder
      ),
      ~ suppressWarnings(as.numeric(.))
    ),
    # canonical, averaged across books (ignore NAs)
    formatted_spread = avg3(
      bovada_formatted_spread, espnbet_formatted_spread, draftkings_formatted_spread
    ),
    formatted_opening_spread = avg3(
      bovada_formatted_opening_spread, espnbet_formatted_opening_spread, draftkings_formatted_opening_spread
    ),
    formatted_overunder = avg3(
      bovada_formatted_overunder, espnbet_formatted_overunder, draftkings_formatted_overunder
    ),
    formatted_opening_overunder = avg3(
      bovada_formatted_opening_overunder, espnbet_formatted_opening_overunder, draftkings_formatted_opening_overunder
    )
  )

combined_df <- combined_df %>%
  mutate(
    startDate = as.POSIXct(startDate, tz = "UTC"),
    total_plays = suppressWarnings(as.numeric(total_plays)),
    formatted_opening_spread = suppressWarnings(as.numeric(formatted_opening_spread)),
    is_completed = !is.na(homePoints) & !is.na(awayPoints)
  )

# 1) Fit simple LMs on COMPLETED games only
lm_pr <- lm(
  passing_rate ~ formatted_opening_spread,
  data = combined_df %>%
    filter(is_completed, !is.na(passing_rate), !is.na(formatted_opening_spread))
)
lm_tp <- lm(
  total_plays ~ formatted_opening_spread,
  data = combined_df %>%
    filter(is_completed, !is.na(total_plays), !is.na(formatted_opening_spread))
)

print(summary(lm_pr))
print(summary(lm_tp))

# 2) Vectorized predictions to avoid predict() dropping rows
b_pr <- coef(lm_pr); b_tp <- coef(lm_tp)
b0_pr <- unname(b_pr["(Intercept)"]);                  b1_pr <- unname(b_pr["formatted_opening_spread"])
b0_tp <- unname(b_tp["(Intercept)"]);                  b1_tp <- unname(b_tp["formatted_opening_spread"])

combined_df <- combined_df %>%
  mutate(
    pr_pred = b0_pr + b1_pr * formatted_opening_spread,
    tp_pred = b0_tp + b1_tp * formatted_opening_spread,
    pr_resid = if_else(is_completed & !is.na(passing_rate), passing_rate - pr_pred, NA_real_),
    tp_resid = if_else(is_completed & !is.na(total_plays),  total_plays  - tp_pred, NA_real_)
  ) %>%
  group_by(team) %>%
  arrange(startDate, .by_group = TRUE) %>%
  mutate(
    # 12-game rolling residuals (past only, unweighted, require 12 completed prior games)
    rolling_passing_rate = slide_dbl(
      lag(pr_resid),
      .f = function(x) {
        x <- x[!is.na(x)]
        if (length(x) < 12) return(NA_real_)
        mean(tail(x, 12))
      },
      .before = 11, .complete = TRUE
    ),
    rolling_total_plays = slide_dbl(
      lag(tp_resid),
      .f = function(x) {
        x <- x[!is.na(x)]
        if (length(x) < 12) return(NA_real_)
        mean(tail(x, 12))
      },
      .before = 11, .complete = TRUE
    )
  ) %>%
  ungroup()

model_df <- combined_df %>% filter(season >= 2021)

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(xgboost)
  library(mgcv)
  library(ParBayesianOptimization)
})

set.seed(2025)

choose_col <- function(df, primary, fallback) {
  if (primary %in% names(df)) {
    return(primary)
  } else if (fallback %in% names(df)) {
    return(fallback)
  } else {
    stop(sprintf("Neither %s nor %s found in dataframe", primary, fallback))
  }
}

spread_col  <- choose_col(model_df, "formatted_spread", "formattedSpread")
ou_col      <- choose_col(model_df, "formatted_overunder", "formattedOverUnder")
ospread_col <- choose_col(model_df, "formatted_opening_spread", "formattedOpeningSpread")
oou_col     <- choose_col(model_df, "formatted_opening_overunder", "formattedOpeningOverUnder")
pts_col     <- choose_col(model_df, "team_points", "points")

model_df <- model_df %>%
  mutate(
    season                       = as.integer(season),
    team_points                  = as.numeric(.data[[pts_col]]),
    formatted_spread             = as.numeric(.data[[spread_col]]),
    formatted_overunder          = as.numeric(.data[[ou_col]]),
    formatted_opening_spread     = as.numeric(.data[[ospread_col]]),
    formatted_opening_overunder  = as.numeric(.data[[oou_col]]),
    
    vegas_opening_team_points = (formatted_opening_overunder + formatted_opening_spread) / 2,
    vegas_team_points         = (formatted_overunder         + formatted_spread)         / 2,
    vegas_opening_opp_points  = formatted_opening_overunder - vegas_opening_team_points,
    vegas_opp_points          = formatted_overunder         - vegas_team_points
  )

swap_cols <- c("offense_effect_overall","offense_effect_passing","offense_effect_rushing",
               "defense_effect_overall","defense_effect_passing","defense_effect_rushing",
               "rolling_passing_rate", "rolling_total_plays")

opp_feats <- model_df %>%
  select(id, team, opponent, season, week, all_of(swap_cols)) %>%
  rename_with(~ paste0("opponent_", .), .cols = all_of(swap_cols)) %>%
  rename(team_swapped = opponent, opponent_swapped = team)

model_df <- model_df %>%
  left_join(
    opp_feats,
    by = c("id","season","week","team" = "team_swapped","opponent" = "opponent_swapped")
  )

features <- c(
  "offense_effect_overall", "offense_effect_passing","offense_effect_rushing",
  "defense_effect_overall","defense_effect_passing","defense_effect_rushing",
  "vegas_opening_team_points", "rolling_passing_rate", "opponent_rolling_passing_rate",
  "rolling_total_plays",
  "team_percentPPA", "opponent_percentPPA",
  "opponent_offense_effect_overall", "opponent_defense_effect_overall"
)

cv_xgb <- model_df %>%
  group_by(id) %>% filter(n()==2) %>% ungroup() %>%
  filter(is.finite(vegas_team_points), is.finite(formatted_spread)) %>%
  mutate(across(all_of(features), as.numeric))

set.seed(2025)

fold_map <- cv_xgb %>% distinct(id) %>% mutate(fold = sample(rep(1:5, length.out = n())))
cv_xgb <- cv_xgb %>% left_join(fold_map, by = "id")

X_all <- as.matrix(cv_xgb[, features])
y_all <- cv_xgb$vegas_team_points
dall  <- xgb.DMatrix(data = X_all, label = y_all, missing = NA)
folds <- lapply(1:5, function(k) which(cv_xgb$fold == k))

xgb_cv_score <- function(max_depth, min_child_weight, subsample, colsample_bytree,
                         eta, gamma, lambda, alpha) {
  params <- list(
    booster          = "gbtree",
    objective        = "reg:squarederror",
    eval_metric      = "rmse",
    max_depth        = as.integer(round(max_depth)),
    min_child_weight = min_child_weight,
    subsample        = subsample,
    colsample_bytree = colsample_bytree,
    eta              = eta,
    gamma            = gamma,
    lambda           = lambda,
    alpha            = alpha
  )
  
  set.seed(2025)
  
  fold_rmses  <- numeric(length(folds))
  fold_bestit <- integer(length(folds))
  
  for (i in seq_along(folds)) {
    val_idx <- folds[[i]]
    tr_idx  <- setdiff(seq_len(nrow(cv_xgb)), val_idx)
    
    dtr  <- xgb.DMatrix(data = X_all[tr_idx, , drop = FALSE], label = y_all[tr_idx], missing = NA)
    dval <- xgb.DMatrix(data = X_all[val_idx, , drop = FALSE], label = y_all[val_idx], missing = NA)
    
    fit <- xgb.train(
      params = params,
      data   = dtr,
      nrounds = 1000,
      watchlist = list(train = dtr, val = dval),
      early_stopping_rounds = 20,
      maximize = FALSE,
      verbose = 1
    )
    
    best_it <- fit$best_iteration
    pred    <- predict(fit, dval, ntreelimit = fit$best_ntreelimit)
    rmse    <- sqrt(mean((pred - y_all[val_idx])^2))
    
    fold_bestit[i] <- best_it
    fold_rmses[i]  <- rmse
  }
  
  best_rmse <- mean(fold_rmses)
  # Use a robust summary for final nrounds suggestion
  best_iter <- as.integer(median(fold_bestit))
  
  list(Score = -best_rmse, nrounds = best_iter)
}

bounds <- list(
  max_depth        = c(3L, 10L),
  min_child_weight = c(1, 10),
  subsample        = c(0.5, 1.0),
  colsample_bytree = c(0.5, 1.0),
  eta              = c(0.01, 0.30),
  gamma            = c(0, 5),
  lambda           = c(0, 5),
  alpha            = c(0, 5)
)

set.seed(2025)

opt <- bayesOpt(
  FUN = xgb_cv_score, bounds = bounds,
  initPoints = 10, iters.n = 100,
  acq = "ei", parallel = FALSE, verbose = 1
)

best_pars <- getBestPars(opt)
best_pars$max_depth <- as.integer(round(best_pars$max_depth))

params_final <- c(
  list(booster="gbtree", objective="reg:squarederror", eval_metric="rmse"),
  best_pars
)


set.seed(2025)
cv_best <- xgb.cv(
  params = params_final, data = dall, nrounds = 1000,
  folds = folds, early_stopping_rounds = 20,
  maximize = FALSE, verbose = 0
)
best_iter <- cv_best$best_iteration

dfull <- xgb.DMatrix(X_all, label = y_all, missing = NA)
final_xgb_fit <- xgb.train(params = params_final, data = dfull, nrounds = best_iter, verbose = 0)

features <- colnames(X_all)

model_bundle <- list(
  raw_model     = xgb.save.raw(final_xgb_fit),
  features_used = features,
  params        = params_final,
  nrounds       = best_iter
)

qsave(model_bundle, "final_xgb_fit.qs")

b <- qs::qread("final_xgb_fit.qs")

if (!is.null(b$raw_model)) {
  tf <- tempfile(fileext = ".xgb")
  writeBin(b$raw_model, tf)
  final_xgb_fit <- xgboost::xgb.load(tf)
} else if (!is.null(b$fit) && inherits(b$fit, "xgb.Booster")) {
  final_xgb_fit <- b$fit
} else if (inherits(b, "xgb.Booster")) {
  final_xgb_fit <- b
} else {
  stop("Could not reconstruct an xgb.Booster from the bundle.")
}

# Optional: pull through metadata you saved
features_used <- b$features_used %||% b$features
params_final  <- b$params %||% NULL
best_iter     <- b$nrounds %||% b$best_iter %||% NA_integer_



eps <- 1e-3

# ------------------------------------------------------------
# 1) 5-fold CV out-of-sample predictions (as you wrote)
# ------------------------------------------------------------
oos_xgb <- bind_rows(lapply(1:5, function(k) {
  train_k <- cv_xgb %>% filter(fold != k)
  test_k  <- cv_xgb %>% filter(fold == k)
  
  dtrain <- xgb.DMatrix(as.matrix(train_k[, features]), label = train_k$vegas_team_points, missing = NA)
  dtest  <- xgb.DMatrix(as.matrix(test_k[,  features]), label = test_k$vegas_team_points,  missing = NA)
  
  set.seed(2025 + k)
  fit_k <- xgb.train(params = params_final, data = dtrain, nrounds = best_iter, verbose = 1)
  preds <- predict(fit_k, dtest)
  
  test_k %>% mutate(pred_team_points = preds, cv_fold = k)
}))

# ------------------------------------------------------------
# 2) Canonicalize lines + points and opponent predictions
# ------------------------------------------------------------
oos_xgb <- oos_xgb %>%
  mutate(
    # team-centric opening spreads (flip sign if home)
    bovada_formatted_opening_spread    = ifelse(is_home == 1L, -bovada_opening_spread,    bovada_opening_spread),
    espnbet_formatted_opening_spread   = ifelse(is_home == 1L, -espnbet_opening_spread,   espnbet_opening_spread),
    draftkings_formatted_opening_spread= ifelse(is_home == 1L, -draftkings_opening_spread,draftkings_opening_spread),
    
    # opening totals (no sign flip)
    bovada_formatted_opening_overunder    = bovada_opening_overunder,
    espnbet_formatted_opening_overunder   = espnbet_opening_overunder,
    draftkings_formatted_opening_overunder= draftkings_opening_overunder,
  )

opp_preds <- oos_xgb %>%
  select(id, team, pred_team_points) %>%
  rename(opponent = team, pred_opponent_points = pred_team_points)

oos_xgb <- oos_xgb %>%
  left_join(opp_preds, by = c("id","opponent")) %>%
  mutate(pred_spread = pred_team_points - pred_opponent_points)

# ------------------------------------------------------------
# 3) Spreads: choose best opening line (max model edge), margins, hit
# ------------------------------------------------------------
oos_xgb <- oos_xgb %>%
  mutate(
    edge_bov  = pred_spread - bovada_formatted_opening_spread,
    edge_espn = pred_spread - espnbet_formatted_opening_spread,
    edge_dk   = pred_spread - draftkings_formatted_opening_spread,
    
    edge_bov  = ifelse(is.na(edge_bov),  -Inf, edge_bov),
    edge_espn = ifelse(is.na(edge_espn), -Inf, edge_espn),
    edge_dk   = ifelse(is.na(edge_dk),   -Inf, edge_dk),
    
    best_idx = max.col(cbind(edge_bov, edge_espn, edge_dk), ties.method = "first"),
    
    best_book_open = dplyr::case_when(
      best_idx == 1L ~ "bovada",
      best_idx == 2L ~ "espnbet",
      best_idx == 3L ~ "draftkings",
      TRUE ~ NA_character_
    ),
    best_line_open = dplyr::case_when(
      best_idx == 1L ~ bovada_formatted_opening_spread,
      best_idx == 2L ~ espnbet_formatted_opening_spread,
      best_idx == 3L ~ draftkings_formatted_opening_spread,
      TRUE ~ NA_real_
    ),
    
    pick_margin  = pred_spread - best_line_open,
    cover_margin = (team_points - opponent_points) - best_line_open,
    
    is_push_actual = abs(cover_margin) <= eps,
    is_push_pick   = abs(pick_margin)  <= eps,
    Hit = ifelse(is_push_actual | is_push_pick, NA, sign(cover_margin) == sign(pick_margin))
  ) %>%
  select(-edge_bov, -edge_espn, -edge_dk, -best_idx)

# ------------------------------------------------------------
# 4) OVER/UNDER (opening totals): pick most favorable book + evaluate
# ------------------------------------------------------------
oos_xgb <- oos_xgb %>%
  mutate(
    pred_total   = pred_team_points + pred_opponent_points,
    actual_total = team_points + opponent_points,
    
    over_bov  = pred_total - bovada_formatted_opening_overunder,
    over_espn = pred_total - espnbet_formatted_opening_overunder,
    over_dk   = pred_total - draftkings_formatted_opening_overunder,
    
    under_bov  = bovada_formatted_opening_overunder - pred_total,
    under_espn = espnbet_formatted_opening_overunder - pred_total,
    under_dk   = draftkings_formatted_opening_overunder - pred_total,
    
    over_bov   = ifelse(is.na(over_bov),  -Inf, over_bov),
    over_espn  = ifelse(is.na(over_espn), -Inf, over_espn),
    over_dk    = ifelse(is.na(over_dk),   -Inf, over_dk),
    under_bov  = ifelse(is.na(under_bov),  -Inf, under_bov),
    under_espn = ifelse(is.na(under_espn), -Inf, under_espn),
    under_dk   = ifelse(is.na(under_dk),   -Inf, under_dk),
    
    best_over_idx  = max.col(cbind(over_bov,  over_espn,  over_dk), ties.method = "first"),
    best_under_idx = max.col(cbind(under_bov, under_espn, under_dk), ties.method = "first"),
    best_over_edge  = pmax(over_bov,  over_espn,  over_dk),
    best_under_edge = pmax(under_bov, under_espn, under_dk),
    
    ou_pick = ifelse(best_over_edge >= best_under_edge, "over", "under"),
    
    best_ou_book_open = dplyr::case_when(
      ou_pick == "over"  & best_over_idx  == 1L ~ "bovada",
      ou_pick == "over"  & best_over_idx  == 2L ~ "espnbet",
      ou_pick == "over"  & best_over_idx  == 3L ~ "draftkings",
      ou_pick == "under" & best_under_idx == 1L ~ "bovada",
      ou_pick == "under" & best_under_idx == 2L ~ "espnbet",
      ou_pick == "under" & best_under_idx == 3L ~ "draftkings",
      TRUE ~ NA_character_
    ),
    best_ou_line_open = dplyr::case_when(
      ou_pick == "over"  & best_over_idx  == 1L ~ bovada_formatted_opening_overunder,
      ou_pick == "over"  & best_over_idx  == 2L ~ espnbet_formatted_opening_overunder,
      ou_pick == "over"  & best_over_idx  == 3L ~ draftkings_formatted_opening_overunder,
      ou_pick == "under" & best_under_idx == 1L ~ bovada_formatted_opening_overunder,
      ou_pick == "under" & best_under_idx == 2L ~ espnbet_formatted_opening_overunder,
      ou_pick == "under" & best_under_idx == 3L ~ draftkings_formatted_opening_overunder,
      TRUE ~ NA_real_
    ),
    
    ou_pick_margin = dplyr::case_when(
      ou_pick == "over"  ~ pred_total   - best_ou_line_open,
      ou_pick == "under" ~ best_ou_line_open - pred_total,
      TRUE ~ NA_real_
    ),
    ou_cover_margin = dplyr::case_when(
      ou_pick == "over"  ~ actual_total - best_ou_line_open,
      ou_pick == "under" ~ best_ou_line_open - actual_total,
      TRUE ~ NA_real_
    ),
    
    ou_is_push_actual = abs(actual_total - best_ou_line_open) <= eps,
    ou_is_push_pick   = abs(ou_pick_margin) <= eps,
    ou_Hit            = ifelse(ou_is_push_actual | ou_is_push_pick, NA, ou_cover_margin > 0)
  ) %>%
  select(-over_bov, -over_espn, -over_dk, -under_bov, -under_espn, -under_dk,
         -best_over_idx, -best_under_idx, -best_over_edge, -best_under_edge)

# ------------------------------------------------------------
# 5) One spread pick per game + spread metrics
# ------------------------------------------------------------
oos_games <- oos_xgb %>%
  group_by(id) %>%
  slice_max(order_by = pick_margin, n = 1, with_ties = FALSE) %>%
  ungroup()

hit_rate_spread <- function(th) {
  tmp <- dplyr::filter(oos_games, !is.na(Hit), abs(pick_margin) >= th)
  if (nrow(tmp)) mean(tmp$Hit) else NA_real_
}
n_at_spread <- function(th) {
  sum(!is.na(oos_games$Hit) & abs(oos_games$pick_margin) >= th)
}

metrics_xgb <- tibble::tibble(
  model       = "xgb",
  MAE_points  = mean(abs(oos_games$pred_team_points - oos_games$team_points), na.rm = TRUE),
  RMSE_points = sqrt(mean((oos_games$pred_team_points - oos_games$team_points)^2, na.rm = TRUE)),
  R2_points   = 1 - sum((oos_games$team_points - oos_games$pred_team_points)^2, na.rm = TRUE) /
    sum((oos_games$team_points - mean(oos_games$team_points, na.rm = TRUE))^2, na.rm = TRUE),
  Hit_overall = mean(oos_games$Hit, na.rm = TRUE)
) %>%
  mutate(
    Hit_abs0_5 = hit_rate_spread(0.5),
    Hit_abs1   = hit_rate_spread(1.0),
    Hit_abs1_5 = hit_rate_spread(1.5),
    Hit_abs2   = hit_rate_spread(2.0),
    Hit_abs2_5 = hit_rate_spread(2.5),
    Hit_abs3   = hit_rate_spread(3.0),
    
    N_overall  = sum(!is.na(oos_games$Hit)),
    N_abs0_5   = n_at_spread(0.5),
    N_abs1     = n_at_spread(1.0),
    N_abs1_5   = n_at_spread(1.5),
    N_abs2     = n_at_spread(2.0),
    N_abs2_5   = n_at_spread(2.5),
    N_abs3     = n_at_spread(3.0)
  )

# ------------------------------------------------------------
# 6) One OU pick per game + totals metrics
# ------------------------------------------------------------
oos_totals <- oos_xgb %>%
  select(
    id, season, week, team, opponent,
    actual_total, pred_total,
    bovada_formatted_opening_overunder, espnbet_formatted_opening_overunder, draftkings_formatted_opening_overunder,
    best_ou_book_open, best_ou_line_open,
    ou_pick, ou_pick_margin, ou_cover_margin, ou_Hit
  ) %>%
  group_by(id) %>%
  slice_max(order_by = ou_pick_margin, n = 1, with_ties = FALSE) %>%
  ungroup()

hit_rate_ou <- function(th) {
  tmp <- dplyr::filter(oos_totals, !is.na(ou_Hit), abs(ou_pick_margin) >= th)
  if (nrow(tmp)) mean(tmp$ou_Hit) else NA_real_
}
n_at_ou <- function(th) {
  sum(!is.na(oos_totals$ou_Hit) & abs(oos_totals$ou_pick_margin) >= th)
}

metrics_ou <- tibble::tibble(
  model          = "xgb_totals",
  MAE_total      = mean(abs(oos_totals$pred_total - oos_totals$actual_total), na.rm = TRUE),
  RMSE_total     = sqrt(mean((oos_totals$pred_total - oos_totals$actual_total)^2, na.rm = TRUE)),
  R2_total       = 1 - sum((oos_totals$actual_total - oos_totals$pred_total)^2, na.rm = TRUE) /
    sum((oos_totals$actual_total - mean(oos_totals$actual_total, na.rm = TRUE))^2, na.rm = TRUE),
  Hit_overall_ou = mean(oos_totals$ou_Hit, na.rm = TRUE)
) %>%
  mutate(
    Hit_abs0_5_ou = hit_rate_ou(0.5),
    Hit_abs1_ou   = hit_rate_ou(1.0),
    Hit_abs1_5_ou = hit_rate_ou(1.5),
    Hit_abs2_ou   = hit_rate_ou(2.0),
    Hit_abs2_5_ou = hit_rate_ou(2.5),
    Hit_abs3_ou   = hit_rate_ou(3.0),
    
    N_overall_ou  = sum(!is.na(oos_totals$ou_Hit)),
    N_abs0_5_ou   = n_at_ou(0.5),
    N_abs1_ou     = n_at_ou(1.0),
    N_abs1_5_ou   = n_at_ou(1.5),
    N_abs2_ou     = n_at_ou(2.0),
    N_abs2_5_ou   = n_at_ou(2.5),
    N_abs3_ou     = n_at_ou(3.0)
  )

importance_matrix <- xgb.importance(feature_names = features, model = final_xgb_fit)
head(importance_matrix, 30)


library(dplyr)
library(ggplot2)
library(scales)

NBINS <- 25

pm_bins <- oos_games %>%
  filter(!is.na(Hit), !is.na(pick_margin)) %>%
  mutate(Hit_num = as.numeric(Hit)) %>%                 # TRUE/FALSE -> 1/0
  mutate(bin = ggplot2::cut_number(pick_margin, NBINS)) %>%  # ~even-sized bins
  group_by(bin) %>%
  summarise(
    pick_margin = median(pick_margin, na.rm = TRUE),    # bin center (median is robust)
    hit_rate    = mean(Hit_num, na.rm = TRUE),
    n           = n(),
    .groups = "drop"
  ) %>%
  arrange(pick_margin)

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
})

# --- generic fitter: quantile-bin then weighted least squares y ~ x ---
fit_units_line <- function(df, x, y, nbins = 25) {
  stopifnot(all(c(x, y) %in% names(df)))
  dat <- df %>%
    filter(!is.na(.data[[x]]), !is.na(.data[[y]])) %>%
    mutate(.y = as.numeric(.data[[y]])) %>%
    mutate(.bin = ggplot2::cut_number(.data[[x]], n = nbins)) %>%
    group_by(.bin) %>%
    summarise(
      x = median(.data[[x]], na.rm = TRUE),   # robust bin center
      y = mean(.y, na.rm = TRUE),             # mean hit rate in bin
      n = n(),
      .groups = "drop"
    ) %>%
    arrange(x)
  
  if (nrow(dat) < 2 || all(!is.finite(dat$y))) {
    return(list(m = NA_real_, b = NA_real_, equation = NA_character_, bins = dat, lm = NULL))
  }
  
  fit <- lm(y ~ x, data = dat, weights = n)
  co  <- coef(fit)
  m   <- unname(co["x"])
  b   <- unname(co["(Intercept)"])
  eq  <- sprintf("y = %.6f * x + %.6f", m, b)
  
  list(m = m, b = b, equation = eq, bins = dat, lm = fit)
}

# --- 1) Spread: Hit vs pick_margin ---
spread_fit <- fit_units_line(oos_games, x = "pick_margin", y = "Hit", nbins = 25)
spread_units_line <- spread_fit$equation
spread_units_line
# e.g. "y = 0.012345 * x + 0.502345"

# --- 2) Totals (OU): ou_Hit vs ou_pick_margin ---
ou_fit <- fit_units_line(oos_games, x = "ou_pick_margin", y = "ou_Hit", nbins = 25)
ou_units_line <- ou_fit$equation
ou_units_line

# (optional) quick plots to sanity-check
if (!is.null(spread_fit$lm)) {
  ggplot(spread_fit$bins, aes(x = x, y = y)) +
    geom_hline(yintercept = 0.524, linetype = 2, linewidth = 0.3) +
    geom_point(aes(size = n), alpha = 0.7) +
    geom_abline(slope = spread_fit$m, intercept = spread_fit$b) +
    labs(x = "Pick margin (points)", y = "Hit rate", title = paste("Spread:", spread_units_line)) +
    scale_y_continuous(limits = c(0,1)) + theme_minimal()
}

if (!is.null(ou_fit$lm)) {
  ggplot(ou_fit$bins, aes(x = x, y = y)) +
    geom_point(aes(size = n), alpha = 0.7) +
    geom_hline(yintercept = 0.524, linetype = 2, linewidth = 0.3) +
    geom_abline(slope = ou_fit$m, intercept = ou_fit$b) +
    labs(x = "OU pick margin (points)", y = "OU hit rate", title = paste("OU:", ou_units_line)) +
    scale_y_continuous(limits = c(0,1)) + theme_minimal()
}

qs::qsave(spread_units_line, "spread_units_line.qs")
qs::qsave(ou_units_line,     "ou_units_line.qs")
