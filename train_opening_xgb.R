source("kalman_filter.R")

con <- dbConnect(
  RPostgres::Postgres(),
  dbname   = "neondb",
  host     = "ep-tiny-fog-aetzb4mp-pooler.c-2.us-east-2.aws.neon.tech",
  port     = 5432,
  user     = "neondb_owner",
  password = "npg_1d0oXImKqyJv",  # or Sys.getenv("NEON_PG_PASS")
  sslmode  = "require"
)

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
    passing_rate
  ) %>%
  distinct(season, week, team, opponent, .keep_all = TRUE)

combined_df <- pregame_latents %>%
  left_join(spread_scores_keys, by = c("season","week","team","opponent"))

combined_df <- combined_df %>%
  group_by(team) %>%
  arrange(season, week, .by_group = TRUE) %>%
  mutate(
    rolling_passing_rate = slider::slide_dbl(
      dplyr::lag(passing_rate),  
      mean, na.rm = TRUE,
      .before = 9,              
      .complete = TRUE        
    )
  ) %>%
  ungroup() %>%
  mutate(adj_offense_effect_passing = rolling_passing_rate * offense_effect_passing,
         adj_offense_effect_rushing = (1 - rolling_passing_rate) * offense_effect_rushing)


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
               "rolling_passing_rate", "adj_offense_effect_passing","adj_offense_effect_rushing")

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
  "offense_effect_overall",#"offense_effect_passing","offense_effect_rushing",
  "defense_effect_overall","defense_effect_passing","defense_effect_rushing",
  "vegas_opening_team_points", "rolling_passing_rate",
  "adj_offense_effect_passing", "adj_offense_effect_rushing",
  "team_percentPPA", "opponent_percentPPA", "week",
  "opponent_rolling_passing_rate", "opponent_offense_effect_overall", "opponent_defense_effect_overall"
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
      verbose = 0
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
  initPoints = 10, iters.n = 10,
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
  folds = folds, early_stopping_rounds = 100,
  maximize = FALSE, verbose = 0
)
best_iter <- cv_best$best_iteration

dfull <- xgb.DMatrix(X_all, label = y_all, missing = NA)
final_xgb_fit <- xgb.train(params = params_final, data = dfull, nrounds = best_iter, verbose = 0)

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

oos_xgb <- oos_xgb %>%
  group_by(id) %>%
  mutate(pred_opponent_points = pred_team_points[match(opponent, team)]) %>%
  ungroup() %>%
  mutate(
    pred_spread    = pred_team_points - pred_opponent_points,
    cover_margin   = score_diff - formatted_opening_spread,
    pick_margin    = pred_spread - formatted_opening_spread,
    is_push_actual = abs(cover_margin) <= eps,
    is_push_pick   = abs(pick_margin)  <= eps,
    Hit            = ifelse(is_push_actual | is_push_pick, NA, sign(cover_margin) == sign(pick_margin))
  )

metrics_xgb <- tibble(
  model       = "xgb",
  MAE_points  = mean(abs(oos_xgb$pred_team_points - oos_xgb$team_points), na.rm = TRUE),
  RMSE_points = sqrt(mean((oos_xgb$pred_team_points - oos_xgb$team_points)^2, na.rm = TRUE)),
  R2_points   = 1 - sum((oos_xgb$team_points - oos_xgb$pred_team_points)^2, na.rm = TRUE) /
    sum((oos_xgb$team_points - mean(oos_xgb$team_points, na.rm = TRUE))^2, na.rm = TRUE),
  Hit_overall = mean(oos_xgb$Hit, na.rm = TRUE),
  Hit_abs1    = oos_xgb %>% filter(!is.na(Hit), abs(pick_margin) >= 1) %>% summarise(h = mean(Hit)) %>% pull(h),
  Hit_abs2    = oos_xgb %>% filter(!is.na(Hit), abs(pick_margin) >= 2) %>% summarise(h = mean(Hit)) %>% pull(h),
  Hit_abs3    = oos_xgb %>% filter(!is.na(Hit), abs(pick_margin) >  3) %>% summarise(h = mean(Hit)) %>% pull(h)
)

n_overall <- oos_xgb %>% filter(!is.na(Hit)) %>% nrow()
n_abs1    <- oos_xgb %>% filter(!is.na(Hit), abs(pick_margin) >= 1) %>% nrow()
n_abs2    <- oos_xgb %>% filter(!is.na(Hit), abs(pick_margin) >= 2) %>% nrow()
n_abs3    <- oos_xgb %>% filter(!is.na(Hit), abs(pick_margin) >  3) %>% nrow()

metrics_xgb <- metrics_xgb %>%
  mutate(
    N_overall = n_overall,
    N_abs1    = n_abs1,
    N_abs2   = n_abs2,
    N_abs3    = n_abs3
  )

metrics_xgb

dfull <- xgb.DMatrix(X_all, label = y_all, missing = NA)
final_xgb_fit <- xgb.train(params = params_final, data = dfull, nrounds = best_iter, verbose = 0)
importance_matrix <- xgb.importance(feature_names = features, model = final_xgb_fit)
head(importance_matrix, 30)
