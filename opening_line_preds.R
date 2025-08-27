suppressPackageStartupMessages({
  library(DBI)
  library(dplyr)
  library(tidyr)
  library(glue)
  library(qs)
  library(xgboost)
  library(slider)
  library(lubridate)
  library(RPostgres)
})

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
MODEL_QS <- "final_xgb_fit.qs"  # saved model bundle (raw binary + features list)
eps <- 1e-3                     # push tolerance for pushes

# ------------------------------------------------------------------------------
# Helpers (define missing pieces so empty global env is OK)
# ------------------------------------------------------------------------------
`%||%` <- function(a, b) if (!is.null(a)) a else b

if (!exists("numFactor", mode = "function")) {
  numFactor <- function(x) as.integer(as.factor(x))
}

# Load kalman pipeline (must define get_pregame_latents())
source("./kalman_filter.R")

# Robust loader for your saved XGB bundle
load_model_bundle <- function(path) {
  b <- qs::qread(path)
  features_used <- b$features_used %||% b$features
  if (is.null(features_used)) stop("Model bundle missing feature list (features_used/features).")
  if (!is.null(b$raw_model)) {
    tf <- tempfile(fileext = ".xgb")
    writeBin(b$raw_model, tf)
    mdl <- xgboost::xgb.load(tf)
  } else if (!is.null(b$fit) && inherits(b$fit, "xgb.Booster")) {
    mdl <- b$fit
  } else if (inherits(b, "xgb.Booster")) {
    mdl <- b
  } else {
    stop("Could not find an xgb.Booster (raw_model/fit) in the bundle.")
  }
  list(model = mdl, features_used = features_used)
}

avg3 <- function(a,b,c){
  n <- (!is.na(a))+(!is.na(b))+(!is.na(c))
  (dplyr::coalesce(a,0)+dplyr::coalesce(b,0)+dplyr::coalesce(c,0)) / ifelse(n==0, NA_real_, n)
}

# ------------------------------------------------------------------------------
# 1) DB connect + pull 2025 rows
# ------------------------------------------------------------------------------
con <- dbConnect(
  RPostgres::Postgres(),
  dbname   = "neondb",
  host     = "ep-tiny-fog-aetzb4mp-pooler.c-2.us-east-2.aws.neon.tech",
  port     = 5432,
  user     = "neondb_owner",
  password = "npg_1d0oXImKqyJv",  # or Sys.getenv("NEON_PG_PASS")
  sslmode  = "require"
)

xgb_2025 <- dbGetQuery(con, 'SELECT * FROM "PreparedData" WHERE season = 2025;')

# ------------------------------------------------------------------------------
# 2) Game indices + basic transforms (mirror training)
# ------------------------------------------------------------------------------
xgb_2025 <- xgb_2025 %>%
  mutate(
    start_dt = suppressWarnings(lubridate::ymd_hms(startDate, quiet = TRUE))
  ) %>%
  arrange(team, season, start_dt, .by_group = FALSE) %>%
  group_by(team, season) %>% mutate(team_game_number = dplyr::row_number()) %>%
  ungroup() %>%
  arrange(opponent, season, start_dt, .by_group = FALSE) %>%
  group_by(opponent, season) %>% mutate(opponent_game_number = dplyr::row_number()) %>%
  ungroup() %>%
  mutate(
    season_num = numFactor(season),
    team_game_num = numFactor(team_game_number),
    opp_game_num  = numFactor(opponent_game_number),
    
    team_f     = factor(team),
    opponent_f = factor(opponent),
    
    passing_plays = suppressWarnings(as.numeric(`offense_passingPlays.totalPPA`)) /
      suppressWarnings(as.numeric(`offense_passingPlays.ppa`)),
    rushing_plays = suppressWarnings(as.numeric(`offense_rushingPlays.totalPPA`)) /
      suppressWarnings(as.numeric(`offense_rushingPlays.ppa`)),
    passing_rate  = passing_plays / (passing_plays + rushing_plays),
    
    points_above_average         = suppressWarnings(as.numeric(offense_ppa)) * 65,
    passing_points_above_average = suppressWarnings(as.numeric(`offense_passingPlays.ppa`)) * 65,
    rushing_points_above_average = suppressWarnings(as.numeric(`offense_rushingPlays.ppa`)) * 65,
    
    is_home = ifelse(homeTeam == team, 1L, 0L),
    
    team_talent     = suppressWarnings(as.numeric(team_talent)),
    opponent_talent = suppressWarnings(as.numeric(opponent_talent))
  ) %>%
  group_by(season) %>%
  mutate(
    team_talent_scaled     = as.numeric(scale(team_talent)),
    opponent_talent_scaled = as.numeric(scale(opponent_talent))
  ) %>% ungroup()

# ------------------------------------------------------------------------------
# 3) percentPPA map for 2025
# ------------------------------------------------------------------------------
ppa_map_2025 <- xgb_2025 %>%
  transmute(
    season = as.integer(season),
    team   = as.character(team_f),
    percentPPA = suppressWarnings(as.numeric(percentPPA))
  ) %>%
  group_by(season, team) %>%
  summarise(percentPPA = dplyr::first(percentPPA[!is.na(percentPPA)]), .groups = "drop")

# ------------------------------------------------------------------------------
# 4) Team-centric per-book lines (same sign convention as training)
# ------------------------------------------------------------------------------
xgb_2025 <- xgb_2025 %>%
  mutate(
    # Bovada
    bovada_formatted_spread = dplyr::case_when(
      is_home == 1L ~ -suppressWarnings(as.numeric(bovada_spread)),
      is_home == 0L ~  suppressWarnings(as.numeric(bovada_spread)),
      TRUE ~ NA_real_),
    bovada_formatted_opening_spread = dplyr::case_when(
      is_home == 1L ~ -suppressWarnings(as.numeric(bovada_opening_spread)),
      is_home == 0L ~  suppressWarnings(as.numeric(bovada_opening_spread)),
      TRUE ~ NA_real_),
    bovada_formatted_overunder         = suppressWarnings(as.numeric(bovada_overunder)),
    bovada_formatted_opening_overunder = suppressWarnings(as.numeric(bovada_opening_overunder)),
    
    # ESPN Bet
    espnbet_formatted_spread = dplyr::case_when(
      is_home == 1L ~ -suppressWarnings(as.numeric(espnbet_spread)),
      is_home == 0L ~  suppressWarnings(as.numeric(espnbet_spread)),
      TRUE ~ NA_real_),
    espnbet_formatted_opening_spread = dplyr::case_when(
      is_home == 1L ~ -suppressWarnings(as.numeric(espnbet_opening_spread)),
      is_home == 0L ~  suppressWarnings(as.numeric(espnbet_opening_spread)),
      TRUE ~ NA_real_),
    espnbet_formatted_overunder         = suppressWarnings(as.numeric(espnbet_overunder)),
    espnbet_formatted_opening_overunder = suppressWarnings(as.numeric(espnbet_opening_overunder)),
    
    # DraftKings
    draftkings_formatted_spread = dplyr::case_when(
      is_home == 1L ~ -suppressWarnings(as.numeric(draftkings_spread)),
      is_home == 0L ~  suppressWarnings(as.numeric(draftkings_spread)),
      TRUE ~ NA_real_),
    draftkings_formatted_opening_spread = dplyr::case_when(
      is_home == 1L ~ -suppressWarnings(as.numeric(draftkings_opening_spread)),
      is_home == 0L ~  suppressWarnings(as.numeric(draftkings_opening_spread)),
      TRUE ~ NA_real_),
    draftkings_formatted_overunder         = suppressWarnings(as.numeric(draftkings_overunder)),
    draftkings_formatted_opening_overunder = suppressWarnings(as.numeric(draftkings_opening_overunder))
  )

# ------------------------------------------------------------------------------
# 5) Team-centric points + percentPPA attach
# ------------------------------------------------------------------------------
spread_scores_keys_2025 <- xgb_2025 %>%
  mutate(
    id       = as.integer(id),
    season   = as.integer(season),
    week     = as.integer(if ("week_num" %in% names(.)) week_num else week),
    team     = as.character(team_f),
    opponent = as.character(opponent_f),
    
    team_points = dplyr::case_when(
      is_home == 1L ~ suppressWarnings(as.numeric(homePoints)),
      is_home == 0L ~ suppressWarnings(as.numeric(awayPoints)),
      TRUE ~ NA_real_
    ),
    opponent_points = dplyr::case_when(
      is_home == 1L ~ suppressWarnings(as.numeric(awayPoints)),
      is_home == 0L ~ suppressWarnings(as.numeric(homePoints)),
      TRUE ~ NA_real_
    ),
    score_diff = team_points - opponent_points
  ) %>%
  left_join(ppa_map_2025 %>% dplyr::rename(team_percentPPA = percentPPA),
            by = c("season", "team")) %>%
  left_join(ppa_map_2025 %>% dplyr::rename(opponent_percentPPA = percentPPA),
            by = c("season" = "season", "opponent" = "team")) %>%
  select(
    id, season, week, team, opponent,
    bovada_formatted_spread, bovada_formatted_opening_spread,
    bovada_formatted_overunder, bovada_formatted_opening_overunder,
    espnbet_formatted_spread, espnbet_formatted_opening_spread,
    espnbet_formatted_overunder, espnbet_formatted_opening_overunder,
    draftkings_formatted_spread, draftkings_formatted_opening_spread,
    draftkings_formatted_overunder, draftkings_formatted_opening_overunder,
    team_points, opponent_points, score_diff,
    team_talent_scaled, opponent_talent_scaled,
    team_percentPPA, opponent_percentPPA,
    passing_rate
  ) %>%
  distinct(season, week, team, opponent, .keep_all = TRUE)

# ------------------------------------------------------------------------------
# 6) Join 2025 latents + rolling pass rate + adj features
# ------------------------------------------------------------------------------
pregame_latents <- get_pregame_latents()

combined_2025 <- pregame_latents %>%
  dplyr::filter(season == 2025) %>%
  dplyr::left_join(spread_scores_keys_2025, by = c("season","week","team","opponent")) %>%
  group_by(team) %>%
  arrange(season, week, .by_group = TRUE) %>%
  mutate(
    rolling_passing_rate = slider::slide_dbl(
      dplyr::lag(passing_rate), mean, na.rm = TRUE,
      .before = 9, .complete = TRUE
    )
  ) %>%
  ungroup() %>%
  mutate(
    adj_offense_effect_passing = rolling_passing_rate * offense_effect_passing,
    adj_offense_effect_rushing = (1 - rolling_passing_rate) * offense_effect_rushing
  )

# ------------------------------------------------------------------------------
# 7) Canonical averaged formatted lines + vegas team points (open/close)
# ------------------------------------------------------------------------------
combined_2025 <- combined_2025 %>%
  mutate(
    formatted_opening_spread = avg3(
      bovada_formatted_opening_spread, espnbet_formatted_opening_spread, draftkings_formatted_opening_spread),
    formatted_opening_overunder = avg3(
      bovada_formatted_opening_overunder, espnbet_formatted_opening_overunder, draftkings_formatted_opening_overunder),
    formatted_spread = avg3(
      bovada_formatted_spread, espnbet_formatted_spread, draftkings_formatted_spread),
    formatted_overunder = avg3(
      bovada_formatted_overunder, espnbet_formatted_overunder, draftkings_formatted_overunder),
    
    vegas_opening_team_points = (formatted_opening_overunder + formatted_opening_spread) / 2,
    vegas_team_points         = (formatted_overunder         + formatted_spread)         / 2
  )

# ------------------------------------------------------------------------------
# 8) Opponent swap features
# ------------------------------------------------------------------------------
swap_cols <- c("offense_effect_overall","offense_effect_passing","offense_effect_rushing",
               "defense_effect_overall","defense_effect_passing","defense_effect_rushing",
               "rolling_passing_rate","adj_offense_effect_passing","adj_offense_effect_rushing")

opp_feats_2025 <- combined_2025 %>%
  dplyr::select(id, team, opponent, season, week, dplyr::all_of(swap_cols)) %>%
  dplyr::rename_with(~ paste0("opponent_", .), .cols = dplyr::all_of(swap_cols)) %>%
  dplyr::rename(team_swapped = opponent, opponent_swapped = team)

combined_2025 <- combined_2025 %>%
  dplyr::left_join(
    opp_feats_2025,
    by = c("id","season","week","team" = "team_swapped","opponent" = "opponent_swapped")
  )

# ------------------------------------------------------------------------------
# 9) Load trained XGBoost and predict 2025 team points
# ------------------------------------------------------------------------------
mdl_bundle <- load_model_bundle(MODEL_QS)
final_xgb_fit <- mdl_bundle$model
features_used <- mdl_bundle$features

missing_feats <- setdiff(features_used, names(combined_2025))
if (length(missing_feats)) combined_2025[missing_feats] <- NA_real_

pred_2025 <- combined_2025 %>%
  dplyr::mutate(across(all_of(features_used), as.numeric))

X_2025 <- as.matrix(pred_2025[, features_used, drop = FALSE])
d2025  <- xgboost::xgb.DMatrix(X_2025, missing = NA)
pred_2025$pred_team_points <- predict(final_xgb_fit, d2025)

# ------------------------------------------------------------------------------
# 10) Opponent predictions, spreads, and most-favorable OPENING spread line
# ------------------------------------------------------------------------------
opp_preds_2025 <- pred_2025 %>%
  dplyr::select(id, team, pred_team_points) %>%
  dplyr::rename(opponent = team, pred_opponent_points = pred_team_points)

pred_2025 <- pred_2025 %>%
  dplyr::left_join(opp_preds_2025, by = c("id", "opponent")) %>%
  dplyr::mutate(pred_spread = pred_team_points - pred_opponent_points)

pred_2025 <- pred_2025 %>%
  mutate(
    n_open_lines = rowSums(!is.na(cbind(
      bovada_formatted_opening_spread,
      espnbet_formatted_opening_spread,
      draftkings_formatted_opening_spread
    ))),
    
    edge_bov  = pred_spread - bovada_formatted_opening_spread,
    edge_espn = pred_spread - espnbet_formatted_opening_spread,
    edge_dk   = pred_spread - draftkings_formatted_opening_spread,
    edge_bov  = ifelse(is.na(edge_bov),  -Inf, edge_bov),
    edge_espn = ifelse(is.na(edge_espn), -Inf, edge_espn),
    edge_dk   = ifelse(is.na(edge_dk),   -Inf, edge_dk),
    
    best_idx  = ifelse(n_open_lines == 0, NA_integer_,
                       max.col(cbind(edge_bov, edge_espn, edge_dk), ties.method = "first")),
    best_book_open = dplyr::case_when(
      is.na(best_idx) ~ NA_character_,
      best_idx == 1L  ~ "bovada",
      best_idx == 2L  ~ "espnbet",
      best_idx == 3L  ~ "draftkings",
      TRUE            ~ NA_character_
    ),
    best_line_open = dplyr::case_when(
      best_idx == 1L ~ bovada_formatted_opening_spread,
      best_idx == 2L ~ espnbet_formatted_opening_spread,
      best_idx == 3L ~ draftkings_formatted_opening_spread,
      TRUE ~ NA_real_
    )
  ) %>%
  dplyr::select(-edge_bov, -edge_espn, -edge_dk, -best_idx)

pred_2025 <- pred_2025 %>%
  dplyr::mutate(
    cover_margin   = ifelse(!is.na(score_diff) & !is.na(best_line_open),
                            score_diff - best_line_open, NA_real_),
    pick_margin    = ifelse(!is.na(best_line_open),
                            pred_spread - best_line_open, NA_real_),
    
    is_push_actual = ifelse(!is.na(cover_margin), abs(cover_margin) <= eps, NA),
    is_push_pick   = ifelse(!is.na(pick_margin),  abs(pick_margin)  <= eps, NA),
    
    Hit = dplyr::case_when(
      is.na(cover_margin) | is.na(is_push_pick) ~ NA,
      is_push_actual | is_push_pick             ~ NA,
      TRUE                                      ~ sign(cover_margin) == sign(pick_margin)
    )
  )

picks_2025 <- pred_2025 %>%
  dplyr::select(
    id, season, week, team, opponent,
    team_points, opponent_points, score_diff,
    pred_team_points, pred_opponent_points, pred_spread,
    bovada_formatted_opening_spread, espnbet_formatted_opening_spread, draftkings_formatted_opening_spread,
    best_book_open, best_line_open, pick_margin, cover_margin, Hit
  ) %>%
  dplyr::group_by(id) %>%
  dplyr::slice_max(order_by = pick_margin, n = 1, with_ties = FALSE) %>%
  dplyr::ungroup()

stopifnot(anyDuplicated(picks_2025$id) == 0)

# ------------------------------------------------------------------------------
# 11) OVER/UNDER predictions (opening totals)
# ------------------------------------------------------------------------------
pred_2025 <- pred_2025 %>%
  dplyr::mutate(
    pred_total   = pred_team_points + pred_opponent_points,
    actual_total = ifelse(!is.na(team_points) & !is.na(opponent_points),
                          team_points + opponent_points, NA_real_),
    
    n_open_ou = rowSums(!is.na(cbind(
      bovada_formatted_opening_overunder,
      espnbet_formatted_opening_overunder,
      draftkings_formatted_opening_overunder
    ))),
    
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
    
    best_over_idx  = ifelse(n_open_ou == 0, NA_integer_,
                            max.col(cbind(over_bov, over_espn, over_dk), ties.method = "first")),
    best_under_idx = ifelse(n_open_ou == 0, NA_integer_,
                            max.col(cbind(under_bov, under_espn, under_dk), ties.method = "first")),
    
    best_over_edge  = pmax(over_bov,  over_espn,  over_dk),
    best_under_edge = pmax(under_bov, under_espn, under_dk),
    
    ou_pick = dplyr::case_when(
      n_open_ou == 0                    ~ NA_character_,
      best_over_edge >= best_under_edge ~ "over",
      TRUE                              ~ "under"
    ),
    
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
    )
  ) %>%
  dplyr::select(-over_bov, -over_espn, -over_dk, -under_bov, -under_espn, -under_dk,
                -best_over_idx, -best_under_idx, -best_over_edge, -best_under_edge)

pred_2025 <- pred_2025 %>%
  dplyr::mutate(
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
    ou_is_push_actual = ifelse(!is.na(best_ou_line_open) & !is.na(actual_total),
                               abs(actual_total - best_ou_line_open) <= eps, NA),
    ou_is_push_pick   = ifelse(!is.na(best_ou_line_open) & !is.na(ou_pick_margin),
                               abs(ou_pick_margin) <= eps, NA),
    ou_Hit = ifelse(ou_is_push_actual | ou_is_push_pick | is.na(best_ou_line_open),
                    NA, ou_cover_margin > 0)
  )

picks_totals_2025 <- pred_2025 %>%
  dplyr::select(
    id, season, week, team, opponent,
    actual_total, pred_total,
    bovada_formatted_opening_overunder, espnbet_formatted_opening_overunder, draftkings_formatted_opening_overunder,
    best_ou_book_open, best_ou_line_open,
    ou_pick, ou_pick_margin, ou_cover_margin, ou_Hit
  ) %>%
  dplyr::group_by(id) %>%
  dplyr::slice_max(order_by = ou_pick_margin, n = 1, with_ties = FALSE) %>%
  dplyr::ungroup()

stopifnot(anyDuplicated(picks_totals_2025$id) == 0)

# ------------------------------------------------------------------------------
# 12) Final 2025 dataframe with BOTH spread + totals (one row per game id)
# ------------------------------------------------------------------------------
final_2025 <- picks_2025 %>%
  dplyr::left_join(
    picks_totals_2025 %>%
      dplyr::select(
        id,
        pred_total, actual_total,
        best_ou_book_open, best_ou_line_open,
        ou_pick, ou_pick_margin, ou_cover_margin, ou_Hit
      ),
    by = "id"
  ) %>%
  dplyr::select(
    id, season, week, team, opponent,
    pred_team_points, pred_opponent_points, pred_spread,
    best_book_open, best_line_open, pick_margin, cover_margin, Hit,
    pred_total, actual_total,
    best_ou_book_open, best_ou_line_open,
    ou_pick, ou_pick_margin, ou_cover_margin, ou_Hit
  ) %>%
  dplyr::arrange(season, week, team)

stopifnot(anyDuplicated(final_2025$id) == 0)

final_2025_out <- final_2025 %>%
  dplyr::mutate(
    team                 = as.character(team),
    opponent             = as.character(opponent),
    best_book_open       = as.character(best_book_open),
    best_ou_book_open    = as.character(best_ou_book_open),
    ou_pick              = as.character(ou_pick)
  )

# ------------------------------------------------------------------------------
# 13) Upsert into public."GamePicks" by id (only update if a row actually changed)
# ------------------------------------------------------------------------------
sync_gamepicks <- function(con, df) {
  stopifnot("id" %in% names(df))
  
  DBI::dbExecute(con, 'ALTER TABLE "GamePicks"
                       ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();')
  
  tbl_cols <- DBI::dbGetQuery(
    con,
    "SELECT column_name
       FROM information_schema.columns
      WHERE table_schema = 'public' AND table_name = 'GamePicks'
      ORDER BY ordinal_position"
  )$column_name
  
  df_norm <- df
  miss <- setdiff(tbl_cols, names(df_norm))
  if (length(miss)) for (cc in miss) df_norm[[cc]] <- NA
  extra <- setdiff(names(df_norm), tbl_cols)
  if (length(extra)) df_norm <- dplyr::select(df_norm, -dplyr::all_of(extra))
  df_norm <- dplyr::select(df_norm, dplyr::all_of(tbl_cols))
  
  coerce_if_present <- function(x, fn) if (x %in% names(df_norm)) df_norm[[x]] <<- fn(df_norm[[x]])
  coerce_if_present("id", as.integer)
  coerce_if_present("season", as.integer)
  coerce_if_present("week", as.integer)
  for (c in c("team","opponent","best_book_open","best_ou_book_open","ou_pick")) {
    coerce_if_present(c, as.character)
  }
  for (c in c("Hit","ou_Hit")) {
    coerce_if_present(c, as.logical)
  }
  
  stage_name <- DBI::Id(schema = NULL, table = "GamePicks_stage")
  if (DBI::dbExistsTable(con, stage_name)) DBI::dbRemoveTable(con, stage_name)
  
  insert_cols <- setdiff(names(df_norm), "updated_at")
  df_stage <- dplyr::select(df_norm, dplyr::all_of(insert_cols))
  DBI::dbWriteTable(con, stage_name, df_stage, temporary = TRUE, overwrite = TRUE)
  
  set_cols <- setdiff(insert_cols, "id")
  
  q_insert_cols <- paste0('"', insert_cols, '"', collapse = ", ")
  q_set_clause  <- paste0('"', set_cols, "\" = EXCLUDED.\"", set_cols, '"', collapse = ", ")
  
  cmp_cols   <- setdiff(set_cols, "updated_at")
  q_cmp_left  <- paste0('"GamePicks"."', cmp_cols, '"', collapse = ", ")
  q_cmp_right <- paste0('EXCLUDED."', cmp_cols, '"', collapse = ", ")
  
  sql_upsert <- glue::glue('
    INSERT INTO "GamePicks" ({q_insert_cols})
    SELECT {q_insert_cols} FROM "GamePicks_stage"
    ON CONFLICT (id) DO UPDATE
    SET {q_set_clause}, "updated_at" = NOW()
    WHERE ROW({q_cmp_left}) IS DISTINCT FROM ROW({q_cmp_right});
  ')
  
  DBI::dbWithTransaction(con, {
    DBI::dbExecute(con, sql_upsert)
    try(DBI::dbRemoveTable(con, stage_name), silent = TRUE)
  })
}

# Run the sync
stopifnot(anyDuplicated(final_2025_out$id) == 0)
sync_gamepicks(con, final_2025_out)

# Optional: dbDisconnect(con)


# Optional: close the connection
# dbDisconnect(con)


# # 1) Create table if it doesn't exist (with PK on id)
# DBI::dbExecute(con, '
#   CREATE TABLE IF NOT EXISTS "GamePicks" (
#     id BIGINT PRIMARY KEY,
#     season INTEGER,
#     week INTEGER,
#     team TEXT,
#     opponent TEXT,
#     pred_team_points DOUBLE PRECISION,
#     pred_opponent_points DOUBLE PRECISION,
#     pred_spread DOUBLE PRECISION,
#     best_book_open TEXT,
#     best_line_open DOUBLE PRECISION,
#     pick_margin DOUBLE PRECISION,
#     cover_margin DOUBLE PRECISION,
#     "Hit" BOOLEAN,
#     pred_total DOUBLE PRECISION,
#     actual_total DOUBLE PRECISION,
#     best_ou_book_open TEXT,
#     best_ou_line_open DOUBLE PRECISION,
#     ou_pick TEXT,
#     ou_pick_margin DOUBLE PRECISION,
#     ou_cover_margin DOUBLE PRECISION,
#     "ou_Hit" BOOLEAN
#   );
# ')
# 
# tbl_cols <- DBI::dbGetQuery(
#   con,
#   "SELECT column_name
#      FROM information_schema.columns
#     WHERE table_schema = 'public' AND table_name = 'GamePicks'
#     ORDER BY ordinal_position"
# )$column_name
# 
# # 2) Start from your df_2025_out and make sure all required columns exist,
# #    drop any extras, coerce key types, and reorder to match the table.
# df_to_write <- final_2025_out
# 
# # add any missing columns as NA to match the table schema
# missing_cols <- setdiff(tbl_cols, names(df_to_write))
# if (length(missing_cols)) {
#   for (cc in missing_cols) df_to_write[[cc]] <- NA
# }
# 
# # drop columns not in the table
# extra_cols <- setdiff(names(df_to_write), tbl_cols)
# if (length(extra_cols)) {
#   df_to_write <- dplyr::select(df_to_write, -dplyr::all_of(extra_cols))
# }
# 
# # reorder to DB column order
# df_to_write <- dplyr::select(df_to_write, dplyr::all_of(tbl_cols))
# 
# # coerce common column types (adjust if your schema differs)
# df_to_write <- df_to_write %>%
#   mutate(
#     id                  = as.integer(id),
#     season              = as.integer(season),
#     week                = as.integer(week),
#     team                = as.character(team),
#     opponent            = as.character(opponent),
#     best_book_open      = as.character(best_book_open),
#     best_ou_book_open   = as.character(best_ou_book_open),
#     ou_pick             = as.character(ou_pick),
#     Hit                 = as.logical(Hit),
#     ou_Hit              = as.logical(ou_Hit)
#   )
# 
# # 3) TRUNCATE then INSERT ALL ROWS (keeps PK & indexes)
# DBI::dbWithTransaction(con, {
#   DBI::dbExecute(con, 'TRUNCATE TABLE "GamePicks";')
#   DBI::dbAppendTable(con, DBI::Id(schema = "public", table = "GamePicks"), df_to_write)
# })
