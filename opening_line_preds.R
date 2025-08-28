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
  library(jsonlite)
  library(glmmTMB)
  library(oddsapiR)
  library(purrr)
  library(stringr)
})

Sys.setenv(ODDS_API_KEY = "cc37e6531ad6f6bb13fd927e81bdf952")

Sys.getenv("ODDS_API_KEY")

toa_requests()

sports <- toa_sports(all_sports = TRUE)
subset(sports, grepl("ncaaf", key, ignore.case = TRUE))

raw <- toa_sports_odds(
  sport_key   = "americanfootball_ncaaf",
  regions     = "us",
  markets     = "spreads,totals",
  odds_format = "american",
  date_format = "iso"
)


team_map <- c(
  # ACC / Independents / Power 5-ish
  "Boston College Eagles"              = "Boston College",
  "Clemson Tigers"                     = "Clemson",
  "Duke Blue Devils"                   = "Duke",
  "Florida State Seminoles"            = "Florida State",
  "Georgia Tech Yellow Jackets"        = "Georgia Tech",
  "Louisville Cardinals"               = "Louisville",
  "Miami Hurricanes"                   = "Miami",
  "North Carolina Tar Heels"           = "North Carolina",
  "NC State Wolfpack"                  = "NC State",
  "Pittsburgh Panthers"                = "Pittsburgh",
  "Syracuse Orange"                    = "Syracuse",
  "Virginia Cavaliers"                 = "Virginia",
  "Virginia Tech Hokies"               = "Virginia Tech",
  "Wake Forest Demon Deacons"          = "Wake Forest",
  "Notre Dame Fighting Irish"          = "Notre Dame",
  
  # Big Ten
  "Illinois Fighting Illini"           = "Illinois",
  "Indiana Hoosiers"                   = "Indiana",
  "Iowa Hawkeyes"                      = "Iowa",
  "Maryland Terrapins"                 = "Maryland",
  "Michigan Wolverines"                = "Michigan",
  "Michigan State Spartans"            = "Michigan State",
  "Minnesota Golden Gophers"           = "Minnesota",
  "Nebraska Cornhuskers"               = "Nebraska",
  "Northwestern Wildcats"              = "Northwestern",
  "Ohio State Buckeyes"                = "Ohio State",
  "Penn State Nittany Lions"           = "Penn State",
  "Purdue Boilermakers"                = "Purdue",
  "Rutgers Scarlet Knights"            = "Rutgers",
  "Wisconsin Badgers"                  = "Wisconsin",
  
  # Big 12
  "Baylor Bears"                       = "Baylor",
  "BYU Cougars"                        = "BYU",
  "Cincinnati Bearcats"                = "Cincinnati",
  "Houston Cougars"                    = "Houston",
  "Iowa State Cyclones"                = "Iowa State",
  "Kansas Jayhawks"                    = "Kansas",
  "Kansas State Wildcats"              = "Kansas State",
  "Oklahoma Sooners"                   = "Oklahoma",
  "Oklahoma State Cowboys"             = "Oklahoma State",
  "TCU Horned Frogs"                   = "TCU",
  "Texas Longhorns"                    = "Texas",
  "Texas Tech Red Raiders"             = "Texas Tech",
  "UCF Knights"                        = "UCF",
  "West Virginia Mountaineers"         = "West Virginia",
  
  # SEC
  "Alabama Crimson Tide"               = "Alabama",
  "Arkansas Razorbacks"                = "Arkansas",
  "Auburn Tigers"                      = "Auburn",
  "Florida Gators"                     = "Florida",
  "Georgia Bulldogs"                   = "Georgia",
  "Kentucky Wildcats"                  = "Kentucky",
  "LSU Tigers"                         = "LSU",
  "Mississippi State Bulldogs"         = "Mississippi State",
  "Missouri Tigers"                    = "Missouri",
  "Ole Miss Rebels"                    = "Ole Miss",
  "South Carolina Gamecocks"           = "South Carolina",
  "Tennessee Volunteers"               = "Tennessee",
  "Texas A&M Aggies"                   = "Texas A&M",
  "Vanderbilt Commodores"              = "Vanderbilt",
  
  # Pac-12 / B1G add-ons
  "Arizona Wildcats"                   = "Arizona",
  "Arizona State Sun Devils"           = "Arizona State",
  "California Golden Bears"            = "California",
  "Colorado Buffaloes"                 = "Colorado",
  "Oregon Ducks"                       = "Oregon",
  "Oregon State Beavers"               = "Oregon State",
  "UCLA Bruins"                        = "UCLA",
  "USC Trojans"                        = "USC",
  "Utah Utes"                          = "Utah",
  "Washington Huskies"                 = "Washington",
  "Washington State Cougars"           = "Washington State",
  "Stanford Cardinal"                  = "Stanford",  # if it appears
  
  # AAC
  "Charlotte 49ers"                    = "Charlotte",
  "East Carolina Pirates"              = "East Carolina",
  "Memphis Tigers"                     = "Memphis",
  "Navy Midshipmen"                    = "Navy",
  "North Texas Mean Green"             = "North Texas",
  "Rice Owls"                          = "Rice",
  "SMU Mustangs"                       = "SMU",
  "South Florida Bulls"                = "South Florida",
  "Temple Owls"                        = "Temple",
  "Tulane Green Wave"                  = "Tulane",
  "Tulsa Golden Hurricane"             = "Tulsa",
  "UAB Blazers"                        = "UAB",
  "UTSA Roadrunners"                   = "UTSA",
  "FAU Owls"                           = "Florida Atlantic",  # if appears
  "USF Bulls"                          = "South Florida",     # alt
  
  # ACC (new members) & others that show up
  "Stanford Cardinal"                  = "Stanford",
  "California Golden Bears"            = "California",
  
  # MWC
  "Air Force Falcons"                  = "Air Force",
  "Boise State Broncos"                = "Boise State",
  "Colorado State Rams"                = "Colorado State",
  "Fresno State Bulldogs"              = "Fresno State",
  "Hawaii Rainbow Warriors"            = "Hawai'i",
  "Nevada Wolf Pack"                   = "Nevada",
  "New Mexico Lobos"                   = "New Mexico",
  "San Diego State Aztecs"             = "San Diego State",
  "San Jose State Spartans"            = "San José State",   # accent in your list
  "San José State Spartans"            = "San José State",   # safety
  "UNLV Rebels"                        = "UNLV",
  "Utah State Aggies"                  = "Utah State",
  "Wyoming Cowboys"                    = "Wyoming",
  
  # Sun Belt
  "Appalachian State Mountaineers"     = "App State",
  "Arkansas State Red Wolves"          = "Arkansas State",
  "Coastal Carolina Chanticleers"      = "Coastal Carolina",
  "Georgia Southern Eagles"            = "Georgia Southern",
  "Georgia State Panthers"             = "Georgia State",
  "James Madison Dukes"                = "James Madison",
  "Louisiana Ragin Cajuns"             = "Louisiana",
  "UL Monroe Warhawks"                 = "UL Monroe",
  "South Alabama Jaguars"              = "South Alabama",
  "Southern Mississippi Golden Eagles" = "Southern Miss",
  "Texas State Bobcats"                = "Texas State",
  "Troy Trojans"                       = "Troy",
  "Old Dominion Monarchs"              = "Old Dominion",
  "Marshall Thundering Herd"           = "Marshall",
  
  # MAC
  "Akron Zips"                         = "Akron",
  "Ball State Cardinals"               = "Ball State",
  "Bowling Green Falcons"              = "Bowling Green",
  "Buffalo Bulls"                      = "Buffalo",
  "Central Michigan Chippewas"         = "Central Michigan",
  "Eastern Michigan Eagles"            = "Eastern Michigan",
  "Kent State Golden Flashes"          = "Kent State",
  "Miami (OH) RedHawks"                = "Miami (OH)",
  "Northern Illinois Huskies"          = "Northern Illinois",
  "Ohio Bobcats"                       = "Ohio",
  "Toledo Rockets"                     = "Toledo",
  "Western Michigan Broncos"           = "Western Michigan",
  
  # C-USA
  "FIU Panthers"                       = "Florida International", # if appears
  "Florida International Panthers"     = "Florida International",
  "Jacksonville State Gamecocks"       = "Jacksonville State",
  "Kennesaw State Owls"                = "Kennesaw State",
  "Liberty Flames"                     = "Liberty",
  "Louisiana Tech Bulldogs"            = "Louisiana Tech",
  "Middle Tennessee Blue Raiders"      = "Middle Tennessee", # if appears
  "New Mexico State Aggies"            = "New Mexico State",
  "Sam Houston State Bearkats"         = "Sam Houston",
  "Sam Houston Bearkats"               = "Sam Houston",      # alt
  "UTEP Miners"                        = "UTEP",
  "Western Kentucky Hilltoppers"       = "Western Kentucky", # if appears
  
  # American/Independents that showed up as opponents
  "UConn Huskies"                      = "UConn",
  "UMass Minutemen"                    = "Massachusetts",
  "Army Black Knights"                 = "Army",
  
  # Pac-12/Big Ten stragglers already mapped above, included here for completeness
  "Washington Huskies"                 = "Washington",
  "Washington State Cougars"           = "Washington State",
  
  # Misc seen in your lists that are FBS in your target list
  "California Golden Bears"            = "California",
  "Missouri State Bears"               = "Missouri State",
  "Delaware Blue Hens"                 = "Delaware"
)

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

if (!exists("short_bm", mode = "function")) {
  short_bm <- function(x) {
    dplyr::recode(x,
                  "DraftKings"   = "dk",
                  "FanDuel"      = "fd",
                  "BetMGM"       = "mgm",
                  .default       = make.names(tolower(x))
    )
  }
}
# if team_map wasn't defined above, fall back to identity map (no standardization)
if (!exists("team_map")) team_map <- setNames(character(0), character(0))
normalize_team <- function(x) {
  out <- unname(team_map[match(x, names(team_map))])
  dplyr::coalesce(out, x)
}

# --- 1) keep only MA-legal bookmakers ---
legal_books_boston <- c("DraftKings","FanDuel","BetMGM")
cfb_raw_legal <- raw %>%
  dplyr::filter(bookmaker %in% legal_books_boston)

# --- 2) standardized game keys (home/away) ---
games_key_std <- cfb_raw_legal %>%
  dplyr::distinct(id, commence_time, home_team, away_team) %>%
  dplyr::mutate(
    home_std = normalize_team(home_team),
    away_std = normalize_team(away_team)
  )

# --- 3) spreads (team perspective; one row per id/book/side) ---
spreads_long <- cfb_raw_legal %>%
  dplyr::filter(market_key == "spreads") %>%
  dplyr::mutate(
    bm   = short_bm(bookmaker),
    side = dplyr::case_when(
      outcomes_name == home_team ~ "home",
      outcomes_name == away_team ~ "away",
      TRUE ~ NA_character_
    )
  ) %>%
  dplyr::select(id, bm, side, spread_pts = outcomes_point, spread_price = outcomes_price)

# --- 4) totals (same number for both sides; keep O/U prices) ---
totals_long <- cfb_raw_legal %>%
  dplyr::filter(market_key == "totals") %>%
  dplyr::mutate(
    bm = short_bm(bookmaker),
    ou = if_else(str_to_lower(outcomes_name) == "over", "over", "under")
  ) %>%
  dplyr::select(id, bm, ou, total_num = outcomes_point, total_price = outcomes_price)

totals_wide_by_book <- totals_long %>%
  dplyr::group_by(id, bm) %>%
  dplyr::summarise(
    total_num         = dplyr::first(total_num[!is.na(total_num)]),
    total_over_price  = dplyr::first(total_price[ou == "over"]),
    total_under_price = dplyr::first(total_price[ou == "under"]),
    .groups = "drop"
  ) %>%
  tidyr::pivot_wider(
    id_cols = id,
    names_from = bm,
    values_from = c(total_num, total_over_price, total_under_price),
    names_sep = "_"
  )

# --- 5) two rows per game with is_home flag ---
home_rows <- games_key_std %>%
  dplyr::transmute(
    id, commence_time,
    team     = home_std,
    opponent = away_std,
    is_home  = TRUE,
    side     = "home"
  )
away_rows <- games_key_std %>%
  dplyr::transmute(
    id, commence_time,
    team     = away_std,
    opponent = home_std,
    is_home  = FALSE,
    side     = "away"
  )
teams_long <- dplyr::bind_rows(home_rows, away_rows)

# --- 6) attach spreads (team perspective) and pivot per book ---
teams_with_spreads <- teams_long %>%
  dplyr::left_join(spreads_long, by = c("id","side")) %>%
  tidyr::pivot_wider(
    id_cols    = c(id, commence_time, team, opponent, is_home),
    names_from = bm,
    values_from = c(spread_pts, spread_price),
    names_sep  = "_"
  )

# --- 7) attach totals; flip spread signs; finalize final_df ---
final_df <- teams_with_spreads %>%
  dplyr::left_join(totals_wide_by_book, by = "id") %>%
  dplyr::mutate(across(matches("_spread_pts$"), ~ -.x)) %>%  # flip points
  dplyr::arrange(commence_time, dplyr::desc(is_home), team)

flip_cols <- grep("(^spread_pts_)|(_spread_pts$)", names(final_df), value = TRUE)
final_df <- final_df %>%
  mutate(across(all_of(flip_cols),
                ~ -suppressWarnings(as.numeric(.))))

# =========================
# Merge onto combined_2025
# =========================

# date-only join keys (normalized to UTC)
xgb_2025_key <- xgb_2025 %>%
  dplyr::mutate(
    start_dt_utc = lubridate::ymd_hms(startDate, tz = "UTC"),
    game_date    = as.Date(start_dt_utc)   # POSIXct has tz='UTC' now
  )

final_df_key <- final_df %>%
  dplyr::mutate(
    commence_dt_utc = lubridate::ymd_hms(commence_time, tz = "UTC"),
    game_date       = as.Date(commence_dt_utc)
  ) %>%
  dplyr::select(-commence_time, -commence_dt_utc) %>%
  dplyr::distinct()


# columns from final_df to carry over
book_cols <- names(final_df_key)
book_cols <- setdiff(book_cols, c("id","game_date","team","opponent"))  # keep is_home + dk/fd/mgm cols

# handy coalescer for same-named columns across frames
coalesce_cols <- function(base, a, b, cols) {
  for (nm in cols) base[[nm]] <- dplyr::coalesce(base[[nm]], a[[nm]], b[[nm]])
  base
}

# strict: date + team + opponent
j_strict <- xgb_2025_key %>%
  dplyr::left_join(final_df_key,
                   by = c("game_date"="game_date","team"="team","opponent"="opponent"),
                   suffix = c("", ".final"))

# fallback A: date + team
j_team <- xgb_2025_key %>%
  dplyr::left_join(final_df_key,
                   by = c("game_date"="game_date","team"="team"),
                   suffix = c("", ".byteam"))

# fallback B: date + opponent
j_opp <- xgb_2025_key %>%
  dplyr::left_join(final_df_key,
                   by = c("game_date"="game_date","opponent"="opponent"),
                   suffix = c("", ".byopp"))

merged_df <- j_strict %>% coalesce_cols(j_team, j_opp, book_cols)


# ------------------------------------------------------------------------------
# 3) percentPPA map for 2025
# ------------------------------------------------------------------------------
ppa_map_2025 <- merged_df %>%
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
library(dplyr)

merged_df <- merged_df %>%
  mutate(
    # ---- OddsAPI books (already team-perspective) ----
    # DraftKings (use ONLY the OddsAPI fields)
    dk_formatted_spread              = suppressWarnings(as.numeric(spread_pts_dk)),
    dk_formatted_spread_price        = suppressWarnings(as.numeric(spread_price_dk)),
    dk_formatted_overunder           = suppressWarnings(as.numeric(total_num_dk)),
    dk_formatted_total_over_price    = suppressWarnings(as.numeric(total_over_price_dk)),
    dk_formatted_total_under_price   = suppressWarnings(as.numeric(total_under_price_dk)),
    
    # FanDuel
    fd_formatted_spread              = suppressWarnings(as.numeric(spread_pts_fd)),
    fd_formatted_spread_price        = suppressWarnings(as.numeric(spread_price_fd)),
    fd_formatted_overunder           = suppressWarnings(as.numeric(total_num_fd)),
    fd_formatted_total_over_price    = suppressWarnings(as.numeric(total_over_price_fd)),
    fd_formatted_total_under_price   = suppressWarnings(as.numeric(total_under_price_fd)),
    
    # BetMGM
    mgm_formatted_spread             = suppressWarnings(as.numeric(spread_pts_mgm)),
    mgm_formatted_spread_price       = suppressWarnings(as.numeric(spread_price_mgm)),
    mgm_formatted_overunder          = suppressWarnings(as.numeric(total_num_mgm)),
    mgm_formatted_total_over_price   = suppressWarnings(as.numeric(total_over_price_mgm)),
    mgm_formatted_total_under_price  = suppressWarnings(as.numeric(total_under_price_mgm)),
    
    # ---- Legacy DB books (need home/away sign) ----
    # Bovada
    bovada_formatted_spread = dplyr::case_when(
      is_home == 1L ~ -suppressWarnings(as.numeric(bovada_spread)),
      is_home == 0L ~  suppressWarnings(as.numeric(bovada_spread)),
      TRUE ~ NA_real_
    ),
    bovada_formatted_opening_spread = dplyr::case_when(
      is_home == 1L ~ -suppressWarnings(as.numeric(bovada_opening_spread)),
      is_home == 0L ~  suppressWarnings(as.numeric(bovada_opening_spread)),
      TRUE ~ NA_real_
    ),
    bovada_formatted_overunder         = suppressWarnings(as.numeric(bovada_overunder)),
    bovada_formatted_opening_overunder = suppressWarnings(as.numeric(bovada_opening_overunder)),
    
    # ESPN Bet
    espnbet_formatted_spread = dplyr::case_when(
      is_home == 1L ~ -suppressWarnings(as.numeric(espnbet_spread)),
      is_home == 0L ~  suppressWarnings(as.numeric(espnbet_spread)),
      TRUE ~ NA_real_
    ),
    espnbet_formatted_opening_spread = dplyr::case_when(
      is_home == 1L ~ -suppressWarnings(as.numeric(espnbet_opening_spread)),
      is_home == 0L ~  suppressWarnings(as.numeric(espnbet_opening_spread)),
      TRUE ~ NA_real_
    ),
    espnbet_formatted_overunder         = suppressWarnings(as.numeric(espnbet_overunder)),
    espnbet_formatted_opening_overunder = suppressWarnings(as.numeric(espnbet_opening_overunder))
  ) %>%
  # Optionally drop the old DB DraftKings fields so you only keep the OddsAPI DK:
  select(-starts_with("draftkings_"))

# ------------------------------------------------------------------------------
# 5) Team-centric points + percentPPA attach
# ------------------------------------------------------------------------------
spread_scores_keys_2025 <- merged_df %>%
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
    id, season, week, team, opponent, startDate,
    
    # DB / legacy books (signed with is_home earlier)
    bovada_formatted_spread, bovada_formatted_opening_spread,
    bovada_formatted_overunder, bovada_formatted_opening_overunder,
    espnbet_formatted_spread, espnbet_formatted_opening_spread,
    espnbet_formatted_overunder, espnbet_formatted_opening_overunder,
    
    # OddsAPI books (team-perspective)
    dk_formatted_spread,          dk_formatted_spread_price,
    dk_formatted_overunder,       dk_formatted_total_over_price,   dk_formatted_total_under_price,
    fd_formatted_spread,          fd_formatted_spread_price,
    fd_formatted_overunder,       fd_formatted_total_over_price,   fd_formatted_total_under_price,
    mgm_formatted_spread,         mgm_formatted_spread_price,
    mgm_formatted_overunder,      mgm_formatted_total_over_price,  mgm_formatted_total_under_price,
    
    # optional averages if you created them
    dplyr::any_of(c("formatted_spread_avg","formatted_total_avg")),
    
    # targets + features
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

avg_row <- function(...) {
  x <- rowMeans(cbind(...), na.rm = TRUE)
  ifelse(is.nan(x), NA_real_, x)
}

combined_2025 <- combined_2025 %>%
  mutate(
    # OPENING lines: only from DB books (Bovada / ESPN Bet)
    formatted_opening_spread = avg_row(
      bovada_formatted_opening_spread,
      espnbet_formatted_opening_spread,
      dk_formatted_spread,
      fd_formatted_spread,
      mgm_formatted_spread
    ),
    formatted_opening_overunder = avg_row(
      bovada_formatted_opening_overunder,
      espnbet_formatted_opening_overunder,
      dk_formatted_overunder,
      fd_formatted_overunder,
      mgm_formatted_overunder
    ),
    
    # CURRENT lines: prefer OddsAPI books (DK/FD/MGM), but also allow DB books if present
    formatted_spread = avg_row(
      dk_formatted_spread,
      fd_formatted_spread,
      mgm_formatted_spread,
      bovada_formatted_spread,
      espnbet_formatted_spread
    ),
    formatted_overunder = avg_row(
      dk_formatted_overunder,
      fd_formatted_overunder,
      mgm_formatted_overunder,
      bovada_formatted_overunder,
      espnbet_formatted_overunder
    ),
    
    # Implied team totals (same formulas you had)
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

# Existing step
combined_2025 <- combined_2025 %>%
  dplyr::left_join(
    opp_feats_2025,
    by = c("id","season","week","team" = "team_swapped","opponent" = "opponent_swapped")
  )

# --- Manual MGM imputes here, then flip for the opponent row of the same id ---

# 1) Define the manual imputes you want (add rows as needed)
manual_mgm_impute <- tibble::tibble(
  season        = 2025L,
  team          = c("Navy"),     # team perspective you want to set
  opponent      = c("Army"),
  mgm_spread_impute = c(-3),     # your imputed line for *this team* row
  mgm_price_impute  = c(-110)    # price for this team row
)

# 2) Attach the manual values to matching rows (by season/team/opponent)
combined_2025 <- combined_2025 %>%
  dplyr::left_join(
    manual_mgm_impute %>% dplyr::select(season, team, opponent, mgm_spread_impute, mgm_price_impute),
    by = c("season","team","opponent")
  ) %>%
  dplyr::mutate(
    # fill only if currently NA
    mgm_formatted_spread       = dplyr::coalesce(mgm_formatted_spread,       as.numeric(mgm_spread_impute)),
    mgm_formatted_spread_price = dplyr::coalesce(mgm_formatted_spread_price, as.numeric(mgm_price_impute))
  ) %>%
  dplyr::select(-mgm_spread_impute, -mgm_price_impute)

# 3) Mirror to the other team row in the same game id:
#    - spread flips sign
#    - price is copied as-is (common case like -110/-110)
combined_2025 <- combined_2025 %>%
  dplyr::group_by(id) %>%
  dplyr::mutate(
    # first non-NA in the group (if any)
    .grp_spread = dplyr::first(na.omit(mgm_formatted_spread)),
    .grp_price  = dplyr::first(na.omit(mgm_formatted_spread_price)),
    mgm_formatted_spread = dplyr::case_when(
      is.na(mgm_formatted_spread) & !is.na(.grp_spread) ~ -.grp_spread,
      TRUE ~ mgm_formatted_spread
    ),
    mgm_formatted_spread_price = dplyr::case_when(
      is.na(mgm_formatted_spread_price) & !is.na(.grp_price) ~ .grp_price,
      TRUE ~ mgm_formatted_spread_price
    )
  ) %>%
  dplyr::ungroup() %>%
  dplyr::select(-.grp_spread, -.grp_price)


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
  dplyr::mutate(
    n_lines = rowSums(!is.na(cbind(
      dk_formatted_spread,
      fd_formatted_spread,
      mgm_formatted_spread
    ))),
    
    edge_dk  = pred_spread - dk_formatted_spread,
    edge_fd  = pred_spread - fd_formatted_spread,
    edge_mgm = pred_spread - mgm_formatted_spread,
    
    # allow max.col to work even with some NAs
    edge_dk  = ifelse(is.na(edge_dk),  -Inf, edge_dk),
    edge_fd  = ifelse(is.na(edge_fd),  -Inf, edge_fd),
    edge_mgm = ifelse(is.na(edge_mgm), -Inf, edge_mgm),
    
    best_idx = dplyr::if_else(
      n_lines == 0, NA_integer_,
      max.col(cbind(edge_dk, edge_fd, edge_mgm), ties.method = "first")
    ),
    
    best_book = dplyr::case_when(
      is.na(best_idx) ~ NA_character_,
      best_idx == 1L  ~ "draftkings",
      best_idx == 2L  ~ "fanduel",
      best_idx == 3L  ~ "betmgm",
      TRUE            ~ NA_character_
    ),
    
    best_line = dplyr::case_when(
      best_idx == 1L ~ dk_formatted_spread,
      best_idx == 2L ~ fd_formatted_spread,
      best_idx == 3L ~ mgm_formatted_spread,
      TRUE ~ NA_real_
    )
    
    , best_price = dplyr::case_when(
        best_idx == 1L ~ dk_formatted_spread_price,
        best_idx == 2L ~ fd_formatted_spread_price,
        best_idx == 3L ~ mgm_formatted_spread_price,
        TRUE ~ NA_real_
      )
  ) %>%
  dplyr::select(-edge_dk, -edge_fd, -edge_mgm, -best_idx)


# ensure eps exists
if (!exists("eps", inherits = TRUE)) eps <- 1e-3

# ------------------------------------------------------------------------------
# 10) Spread picks (current lines: DK / FD / MGM)
# ------------------------------------------------------------------------------

library(dplyr)
library(stringr)

# Ensure optional ESPN Bet columns exist (so selects & cbind won't error)
for (nm in c(
  "espnbet_formatted_spread", "espnbet_formatted_spread_price",
  "espnbet_formatted_overunder",
  "espnbet_formatted_total_over_price", "espnbet_formatted_total_under_price"
)) {
  if (!nm %in% names(pred_2025)) pred_2025[[nm]] <- NA_real_
}

# -----------------------------
# 10) Spread picks (current lines with price-aware tie-break; DK/FD/MGM/ESPN)
# -----------------------------
pred_2025 <- pred_2025 %>%
  mutate(
    # how many usable spread lines
    n_lines = rowSums(!is.na(cbind(
      dk_formatted_spread,
      fd_formatted_spread,
      mgm_formatted_spread,
      espnbet_formatted_spread
    )))
  ) %>%
  rowwise() %>%
  mutate(
    # choose best spread book by largest edge; break ties by better (numerically larger) price
    best_book = {
      if (n_lines == 0) NA_character_ else {
        lines  <- c(dk_formatted_spread, fd_formatted_spread, mgm_formatted_spread, espnbet_formatted_spread)
        prices <- c(dk_formatted_spread_price, fd_formatted_spread_price, mgm_formatted_spread_price, espnbet_formatted_spread_price)
        books  <- c("draftkings", "fanduel", "betmgm", "espnbet")
        edges  <- pred_spread - lines
        edges[is.na(edges)] <- -Inf
        be   <- max(edges)
        cand <- which(edges >= be - 1e-9)
        if (length(cand) > 1) {
          pr <- prices[cand]
          if (all(is.na(pr))) books[cand[1]] else books[cand[which.max(pr)]]
        } else books[cand[1]]
      }
    },
    best_line = {
      if (n_lines == 0) NA_real_ else {
        lines  <- c(dk_formatted_spread, fd_formatted_spread, mgm_formatted_spread, espnbet_formatted_spread)
        prices <- c(dk_formatted_spread_price, fd_formatted_spread_price, mgm_formatted_spread_price, espnbet_formatted_spread_price)
        edges  <- pred_spread - lines
        edges[is.na(edges)] <- -Inf
        be   <- max(edges)
        cand <- which(edges >= be - 1e-9)
        if (length(cand) > 1) {
          pr <- prices[cand]
          if (all(is.na(pr))) lines[cand[1]] else lines[cand[which.max(pr)]]
        } else lines[cand[1]]
      }
    }
  ) %>%
  ungroup() %>%
  mutate(
    # cover/pick margins and push logic vs chosen spread
    cover_margin   = ifelse(!is.na(score_diff) & !is.na(best_line), score_diff - best_line, NA_real_),
    pick_margin    = ifelse(!is.na(best_line), pred_spread - best_line, NA_real_),
    is_push_actual = ifelse(!is.na(cover_margin), abs(cover_margin) <= eps, NA),
    is_push_pick   = ifelse(!is.na(pick_margin),  abs(pick_margin)  <= eps, NA),
    Hit = case_when(
      is.na(cover_margin) | is.na(is_push_pick) ~ NA,
      is_push_actual | is_push_pick             ~ NA,
      TRUE                                      ~ sign(cover_margin) == sign(pick_margin)
    )
  )

picks_2025 <- pred_2025 %>%
  select(
    id, startDate, season, week, team, opponent,
    team_points, opponent_points, score_diff,
    pred_team_points, pred_opponent_points, pred_spread,
    best_book, best_line, pick_margin, cover_margin, Hit,
    
    # ----- per-book SPREAD (team-perspective) -----
    dk_formatted_spread,  dk_formatted_spread_price,
    fd_formatted_spread,  fd_formatted_spread_price,
    mgm_formatted_spread, mgm_formatted_spread_price,
    espnbet_formatted_spread, espnbet_formatted_spread_price,
    
    # ----- per-book TOTAL (same number for both teams; include O/U prices) -----
    dk_formatted_overunder,  dk_formatted_total_over_price,  dk_formatted_total_under_price,
    fd_formatted_overunder,  fd_formatted_total_over_price,  fd_formatted_total_under_price,
    mgm_formatted_overunder, mgm_formatted_total_over_price, mgm_formatted_total_under_price,
    espnbet_formatted_overunder, espnbet_formatted_total_over_price, espnbet_formatted_total_under_price
  ) %>%
  group_by(id) %>%
  slice_max(order_by = pick_margin, n = 1, with_ties = FALSE) %>%
  ungroup()

stopifnot(anyDuplicated(picks_2025$id) == 0)

# helper: safe max that returns NA (no warning) if everything is NA
safe_max_vec <- function(x) {
  x <- x[!is.na(x)]
  if (length(x) == 0) NA_real_ else max(x)
}

# -----------------------------
# 11) OVER/UNDER predictions (current totals: DK / FD / MGM / ESPN) — no warnings
# -----------------------------
pred_2025 <- pred_2025 %>%
  dplyr::mutate(
    pred_total   = pred_team_points + pred_opponent_points,
    actual_total = dplyr::if_else(!is.na(team_points) & !is.na(opponent_points),
                                  team_points + opponent_points, NA_real_),
    n_ou_lines = rowSums(!is.na(cbind(
      dk_formatted_overunder,
      fd_formatted_overunder,
      mgm_formatted_overunder,
      espnbet_formatted_overunder
    )))
  ) %>%
  dplyr::rowwise() %>%
  dplyr::mutate(
    # pick direction by comparing best available edges, safely
    best_over_edge  = if (n_ou_lines == 0 || is.na(pred_total)) NA_real_ else safe_max_vec(c(
      pred_total - dk_formatted_overunder,
      pred_total - fd_formatted_overunder,
      pred_total - mgm_formatted_overunder,
      pred_total - espnbet_formatted_overunder
    )),
    best_under_edge = if (n_ou_lines == 0 || is.na(pred_total)) NA_real_ else safe_max_vec(c(
      dk_formatted_overunder - pred_total,
      fd_formatted_overunder - pred_total,
      mgm_formatted_overunder - pred_total,
      espnbet_formatted_overunder - pred_total
    )),
    ou_pick = dplyr::case_when(
      n_ou_lines == 0 ~ NA_character_,
      is.na(best_over_edge) & is.na(best_under_edge) ~ NA_character_,
      best_over_edge >= best_under_edge ~ "over",
      TRUE ~ "under"
    ),
    
    # choose book with price tiebreak for chosen direction
    best_ou_book = {
      if (n_ou_lines == 0 || is.na(ou_pick)) NA_character_ else {
        totals <- c(dk_formatted_overunder, fd_formatted_overunder, mgm_formatted_overunder, espnbet_formatted_overunder)
        books  <- c("draftkings","fanduel","betmgm","espnbet")
        if (ou_pick == "over") {
          edges  <- pred_total - totals
          prices <- c(dk_formatted_total_over_price, fd_formatted_total_over_price, mgm_formatted_total_over_price, espnbet_formatted_total_over_price)
        } else {
          edges  <- totals - pred_total
          prices <- c(dk_formatted_total_under_price, fd_formatted_total_under_price, mgm_formatted_total_under_price, espnbet_formatted_total_under_price)
        }
        edges[is.na(edges)] <- -Inf
        be   <- max(edges)
        cand <- which(edges >= be - 1e-9)
        if (length(cand) > 1) {
          pr <- prices[cand]
          if (all(is.na(pr))) books[cand[1]] else books[cand[which.max(pr)]]
        } else books[cand[1]]
      }
    },
    best_ou_line = {
      if (n_ou_lines == 0 || is.na(ou_pick)) NA_real_ else {
        totals <- c(dk_formatted_overunder, fd_formatted_overunder, mgm_formatted_overunder, espnbet_formatted_overunder)
        if (ou_pick == "over") {
          edges  <- pred_total - totals
          prices <- c(dk_formatted_total_over_price, fd_formatted_total_over_price, mgm_formatted_total_over_price, espnbet_formatted_total_over_price)
        } else {
          edges  <- totals - pred_total
          prices <- c(dk_formatted_total_under_price, fd_formatted_total_under_price, mgm_formatted_total_under_price, espnbet_formatted_total_under_price)
        }
        edges[is.na(edges)] <- -Inf
        be   <- max(edges)
        cand <- which(edges >= be - 1e-9)
        if (length(cand) > 1) {
          pr <- prices[cand]
          if (all(is.na(pr))) totals[cand[1]] else totals[cand[which.max(pr)]]
        } else totals[cand[1]]
      }
    }
  ) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(
    ou_pick_margin = dplyr::case_when(
      ou_pick == "over"  ~ pred_total   - best_ou_line,
      ou_pick == "under" ~ best_ou_line - pred_total,
      TRUE ~ NA_real_
    ),
    ou_cover_margin = dplyr::case_when(
      ou_pick == "over"  ~ actual_total - best_ou_line,
      ou_pick == "under" ~ best_ou_line - actual_total,
      TRUE ~ NA_real_
    ),
    ou_is_push_actual = ifelse(!is.na(best_ou_line) & !is.na(actual_total),
                               abs(actual_total - best_ou_line) <= eps, NA),
    ou_is_push_pick   = ifelse(!is.na(best_ou_line) & !is.na(ou_pick_margin),
                               abs(ou_pick_margin) <= eps, NA),
    ou_Hit = ifelse(ou_is_push_actual | ou_is_push_pick | is.na(best_ou_line),
                    NA, ou_cover_margin > 0)
  )

picks_totals_2025 <- pred_2025 %>%
  select(
    id, startDate, season, week, team, opponent,
    actual_total, pred_total,
    best_ou_book, best_ou_line,
    ou_pick, ou_pick_margin, ou_cover_margin, ou_Hit
  ) %>%
  group_by(id) %>%
  slice_max(order_by = ou_pick_margin, n = 1, with_ties = FALSE) %>%
  ungroup()

stopifnot(anyDuplicated(picks_totals_2025$id) == 0)

# -----------------------------
# Helpers for flipping & pretty strings
# -----------------------------
flip_spread_lines <- function(df) {
  spread_like  <- grep("spread", names(df), value = TRUE)
  spread_lines <- setdiff(spread_like, grep("price|total", spread_like, value = TRUE))
  spread_lines <- unique(c(spread_lines, intersect(c("best_line", "pred_spread"), names(df))))
  if (length(spread_lines)) {
    df <- df %>% mutate(across(all_of(spread_lines), ~ -suppressWarnings(as.numeric(.))))
  }
  attr(df, "flipped_spread_cols") <- spread_lines
  df
}
fmt_line <- function(x) {
  ifelse(
    is.na(x), NA_character_,
    ifelse(x > 0, paste0("+", format(round(x, 1), nsmall = 1)),
           format(round(x, 1), nsmall = 1))
  )
}

# Flip spreads in picks_2025 and add readable picks
picks_2025 <- flip_spread_lines(picks_2025)

picks_2025 <- picks_2025 %>%
  mutate(
    spread_pick = ifelse(
      is.na(team) | is.na(best_line),
      NA_character_,
      paste0(team, " ", fmt_line(best_line))
    )
  ) %>%
  # bring in OU decision text
  left_join(
    picks_totals_2025 %>% select(id, ou_pick, best_ou_line),
    by = "id"
  ) %>%
  mutate(
    ou_pick_str = ifelse(
      is.na(ou_pick) | is.na(best_ou_line),
      NA_character_,
      paste0(str_to_title(ou_pick), " ", format(round(best_ou_line, 1), nsmall = 1))
    )
  )

final_2025 <- picks_2025 %>%
  dplyr::left_join(
    picks_totals_2025 %>%
      dplyr::select(
        id,
        best_ou_book, best_ou_line, ou_pick,  # keep ou_pick; we’ll coalesce
        ou_pick_margin, ou_cover_margin, ou_Hit,
        pred_total, actual_total
      ),
    by = "id",
    suffix = c("", ".tot")
  ) %>%
  # prefer the value already in picks_2025, else take the one from picks_totals_2025
  dplyr::mutate(
    ou_pick      = dplyr::coalesce(ou_pick, ou_pick.tot),
    best_ou_line = dplyr::coalesce(best_ou_line, best_ou_line.tot)
  ) %>%
  dplyr::select(-dplyr::ends_with(".tot")) %>%
  # pretty strings
  dplyr::mutate(
    spread_pick = dplyr::if_else(
      is.na(team) | is.na(best_line), NA_character_,
      paste0(team, " ", ifelse(best_line > 0, paste0("+", format(round(best_line, 1), nsmall = 1)),
                               format(round(best_line, 1), nsmall = 1)))
    ),
    ou_pick_str = dplyr::if_else(
      is.na(ou_pick) | is.na(best_ou_line), NA_character_,
      paste0(stringr::str_to_title(ou_pick), " ", format(round(best_ou_line, 1), nsmall = 1))
    )
  ) %>%
  dplyr::select(
    id, startDate, season, week, team, opponent,
    pred_team_points, pred_opponent_points, pred_spread,
    best_book, best_line, pick_margin, cover_margin, Hit, spread_pick,
    pred_total, actual_total, best_ou_book, best_ou_line,
    ou_pick, ou_pick_margin, ou_cover_margin, ou_Hit, ou_pick_str,
    dk_formatted_spread,      dk_formatted_spread_price,
    fd_formatted_spread,      fd_formatted_spread_price,
    mgm_formatted_spread,     mgm_formatted_spread_price,
    espnbet_formatted_spread, espnbet_formatted_spread_price,
    dk_formatted_overunder,      dk_formatted_total_over_price,      dk_formatted_total_under_price,
    fd_formatted_overunder,      fd_formatted_total_over_price,      fd_formatted_total_under_price,
    mgm_formatted_overunder,     mgm_formatted_total_over_price,     mgm_formatted_total_under_price,
    espnbet_formatted_overunder, espnbet_formatted_total_over_price, espnbet_formatted_total_under_price
  ) %>%
  dplyr::arrange(season, week, team)

load_units_line <- function(path) {
  obj <- qs::qread(path)
  # if saved as list with m/b (from the optional meta save)
  if (is.list(obj) && all(c("m","b") %in% names(obj))) {
    return(list(m = as.numeric(obj$m), b = as.numeric(obj$b)))
  }
  # if saved as plain numeric vector c(m, b)
  if (is.numeric(obj) && length(obj) >= 2) {
    return(list(m = as.numeric(obj[1]), b = as.numeric(obj[2])))
  }
  # if saved as the equation string
  if (is.character(obj) && length(obj) == 1) {
    # expected like: "y = 0.012345 * x + 0.502345"
    m <- suppressWarnings(as.numeric(str_match(obj, "y\\s*=\\s*([+-]?[0-9\\.eE]+)\\s*\\*\\s*x")[,2]))
    b <- suppressWarnings(as.numeric(str_match(obj, "\\+\\s*([+-]?[0-9\\.eE]+)\\s*$")[,2]))
    if (!is.na(m) && !is.na(b)) return(list(m = m, b = b))
  }
  stop(sprintf("Could not parse units line from %s", path))
}

spread_coefs <- load_units_line("spread_units_line.qs")
ou_coefs     <- load_units_line("ou_units_line.qs")

# Compute probabilities on final_2025 (requires pick_margin & ou_pick_margin present)
final_2025 <- final_2025 %>%
  mutate(
    spread_hit_probability = pmin(
      pmax(spread_coefs$m * as.numeric(pick_margin)     + spread_coefs$b, 0), 1
    ),
    ou_hit_probability     = pmin(
      pmax(ou_coefs$m     * as.numeric(ou_pick_margin) + ou_coefs$b,     0), 1
    )
  )

calc_units <- function(p) {
  ifelse(
    is.na(p),
    NA_integer_,
    {
      # 0 if < 52.5%
      # 1 for [0.53,0.55), 2 for [0.55,0.57), 3 for [0.57,0.59), 4 for [0.59,0.61), 5 for >= 0.61
      n <- floor((p - 0.53) / 0.02) + 1L
      ifelse(p < 0.525, 0L, pmin(5L, pmax(0L, n)))
    }
  )
}

final_2025 <- final_2025 %>%
  dplyr::mutate(
    spread_unit = calc_units(spread_hit_probability),
    ou_unit     = calc_units(ou_hit_probability)
  )


final_2025_out <- final_2025 %>%
  dplyr::mutate(
    team          = as.character(team),
    opponent      = as.character(opponent),
    best_book     = as.character(best_book),
    best_ou_book  = as.character(best_ou_book),
    ou_pick       = as.character(ou_pick)
  )


suppressPackageStartupMessages({
  library(DBI)
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(glue)
  library(jsonlite)
  library(httr)
  library(lubridate)
})

# ------------------------------------------------------------------
# 0) Webhook setup (fallback to provided URL if the env var is empty)
# ------------------------------------------------------------------
GAME_PICK_WEBHOOK <- Sys.getenv("GAME_PICK_WEBHOOK", unset = "")
if (!nzchar(GAME_PICK_WEBHOOK)) {
  GAME_PICK_WEBHOOK <- "https://discord.com/api/webhooks/1410096344093167777/uvLW1ZSOs0oEqCmAye8GaRKE3cCyVJj2bYFxgeEehiRCkTZnRsZe6CWczxKA7cwU8Bul"
}

# ------------------------------------------------------------------
# 1) Discord sender with rate-limit handling (429) + retries
# ------------------------------------------------------------------
send_discord <- function(webhook, content = NULL, embeds = NULL,
                         username = "OpeningLine Bot", max_retries = 5L) {
  if (is.na(webhook) || !nzchar(webhook)) return(FALSE)
  payload <- list(username = username)
  if (!is.null(content)) payload$content <- content
  if (!is.null(embeds))  payload$embeds  <- embeds
  
  attempt <- 1L
  repeat {
    resp <- try(
      httr::POST(webhook,
                 httr::add_headers(`Content-Type` = "application/json"),
                 body = jsonlite::toJSON(payload, auto_unbox = TRUE)),
      silent = TRUE
    )
    if (inherits(resp, "try-error")) return(FALSE)
    
    sc <- httr::status_code(resp)
    if (sc == 204 || sc == 200) return(TRUE)
    
    if (sc == 429) {
      body_txt <- httr::content(resp, as = "text", encoding = "UTF-8")
      retry_after <- tryCatch({
        x <- jsonlite::fromJSON(body_txt)
        as.numeric(x$retry_after)
      }, error = function(e) 1)
      Sys.sleep(max(1, retry_after))
      attempt <- attempt + 1L
      if (attempt > max_retries) return(FALSE)
      next
    }
    
    Sys.sleep(0.5)
    attempt <- attempt + 1L
    if (attempt > max_retries) return(FALSE)
  }
}

# ------------------------------------------------------------------
# 2) Ensure audit table exists
# ------------------------------------------------------------------
ensure_lines_seen_table <- function(con) {
  DBI::dbExecute(con, '
    CREATE TABLE IF NOT EXISTS "GamePicksLineSeen" (
      id BIGINT NOT NULL,
      field TEXT NOT NULL,
      first_value DOUBLE PRECISION,
      price_spread DOUBLE PRECISION,
      price_over DOUBLE PRECISION,
      price_under DOUBLE PRECISION,
      first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (id, field)
    );
  ')
}

# ------------------------------------------------------------------
# Helpers for Discord fields (strictly valid embeds)
# ------------------------------------------------------------------
sanitize_str <- function(x, max_nchar) {
  x <- ifelse(is.na(x) | !nzchar(x), "NA", as.character(x))
  ifelse(nchar(x, type = "width") > max_nchar, substr(x, 1, max_nchar), x)
}
make_fields <- function(df) {
  n <- nrow(df); out <- vector("list", n)
  for (i in seq_len(n)) {
    nm  <- sanitize_str(df$name[i], 256)
    val <- sanitize_str(df$value[i], 1024)
    inl <- if ("inline" %in% names(df)) isTRUE(df$inline[i]) else FALSE
    out[[i]] <- list(name = nm, value = val, inline = inl)
  }
  out
}
plus <- function(x, dig = 2) {
  x <- suppressWarnings(as.numeric(x))
  ifelse(is.na(x), "NA",
         ifelse(x >= 0, paste0("+", format(round(x, dig), nsmall = dig)),
                format(round(x, dig), nsmall = dig)))
}

# ------------------------------------------------------------------
# 3) NA -> non-NA detector and Discord notifier
# ------------------------------------------------------------------
post_new_line_picks_to_discord <- function(con, webhook = GAME_PICK_WEBHOOK) {
  stopifnot(DBI::dbIsValid(con))
  ensure_lines_seen_table(con)
  
  # Which columns exist right now?
  gp_cols <- DBI::dbGetQuery(
    con,
    "SELECT column_name FROM information_schema.columns
     WHERE table_schema='public' AND table_name='GamePicks'"
  )$column_name
  
  # Lines we watch (only those present)
  line_cols_all <- c(
    "dk_formatted_spread","fd_formatted_spread","mgm_formatted_spread","espnbet_formatted_spread",
    "dk_formatted_overunder","fd_formatted_overunder","mgm_formatted_overunder","espnbet_formatted_overunder"
  )
  line_cols <- intersect(line_cols_all, gp_cols)
  if (!length(line_cols)) return(invisible(list(notified = 0L, inserted = 0L)))
  
  # Pick/pred columns for messages (only pull if present)
  pick_cols_all <- c(
    "spread_pick","best_book","pick_margin","spread_unit",
    "ou_pick","ou_pick_str","best_ou_book","ou_pick_margin","ou_unit",
    "season","week","team","opponent","startDate"
  )
  pick_cols <- intersect(pick_cols_all, gp_cols)
  
  # Pull GamePicks rows with everything we might need
  sel_cols <- unique(c("id", line_cols, pick_cols))
  q_cols   <- paste(sprintf('"%s"', sel_cols), collapse = ", ")
  gp <- DBI::dbGetQuery(con, sprintf('SELECT %s FROM "GamePicks";', q_cols))
  
  # Long-ify current non-NA lines
  cur_long <- gp %>%
    tidyr::pivot_longer(
      cols = tidyselect::all_of(line_cols),
      names_to = "field",
      values_to = "line"
    ) %>%
    dplyr::filter(!is.na(line)) %>%
    dplyr::select(id, field, line)
  
  if (!nrow(cur_long)) return(invisible(list(notified = 0L, inserted = 0L)))
  
  # What have we already announced?
  seen <- DBI::dbGetQuery(con, 'SELECT id, field FROM "GamePicksLineSeen";')
  
  # Newly non-NA = present now but not in seen
  new_rows <- cur_long %>% dplyr::anti_join(seen, by = c("id","field"))
  if (!nrow(new_rows)) return(invisible(list(notified = 0L, inserted = 0L)))
  
  # Determine which game IDs have new spreads vs new totals
  new_spread_ids <- new_rows %>% dplyr::filter(grepl("spread", field))    %>% dplyr::pull(id) %>% unique()
  new_total_ids  <- new_rows %>% dplyr::filter(grepl("overunder", field)) %>% dplyr::pull(id) %>% unique()
  
  # Build spread items (ordered by spread_unit desc, then pick_margin desc)
  spreads_fields <- list()
  if (length(new_spread_ids)) {
    cols_needed <- c("id","season","week","team","opponent","spread_pick","best_book","pick_margin","spread_unit")
    cols_needed <- intersect(cols_needed, names(gp))
    if (length(cols_needed)) {
      df <- gp %>%
        dplyr::filter(id %in% new_spread_ids) %>%
        dplyr::select(dplyr::all_of(cols_needed)) %>%
        dplyr::mutate(
          spread_unit = as.integer(spread_unit),
          pick_margin = suppressWarnings(as.numeric(pick_margin)),
          title = sprintf("SPREAD — %s vs %s (Wk %s, %s)",
                          team %||% "NA", opponent %||% "NA",
                          as.character(week %||% NA), as.character(season %||% NA)),
          value = sprintf("%s  |  %s  |  edge %s  |  units %s",
                          spread_pick %||% "NA",
                          best_book %||% "NA",
                          plus(pick_margin, 2),
                          ifelse(is.na(spread_unit), "NA", as.character(spread_unit)))
        ) %>%
        dplyr::transmute(name = title, value = value, inline = FALSE)
      spreads_fields <- make_fields(df)
    }
  }
  
  # Build total items (ordered by ou_unit desc, then ou_pick_margin desc)
  totals_fields <- list()
  if (length(new_total_ids)) {
    cols_needed <- c("id","season","week","team","opponent","ou_pick","ou_pick_str","best_ou_book","ou_pick_margin","ou_unit")
    cols_needed <- intersect(cols_needed, names(gp))
    if (length(cols_needed)) {
      df <- gp %>%
        dplyr::filter(id %in% new_total_ids) %>%
        dplyr::select(dplyr::all_of(cols_needed)) %>%
        dplyr::mutate(
          ou_unit        = as.integer(ou_unit),
          ou_pick_margin = suppressWarnings(as.numeric(ou_pick_margin)),
          title = sprintf("TOTAL — %s vs %s (Wk %s, %s)",
                          team %||% "NA", opponent %||% "NA",
                          as.character(week %||% NA), as.character(season %||% NA)),
          pick_str = dplyr::coalesce(ou_pick_str,
                                     ifelse(is.na(ou_pick), "(no pick)",
                                            paste0(stringr::str_to_title(ou_pick), " ?"))),
          value = sprintf("%s  |  %s  |  edge %s  |  units %s",
                          pick_str %||% "NA",
                          best_ou_book %||% "NA",
                          plus(ou_pick_margin, 2),
                          ifelse(is.na(ou_unit), "NA", as.character(ou_unit)))
        ) %>%
        dplyr::transmute(name = title, value = value, inline = FALSE)
      totals_fields <- make_fields(df)
    }
  }
  
  fields_all <- c(spreads_fields, totals_fields)
  if (!length(fields_all)) return(invisible(list(notified = 0L, inserted = nrow(new_rows))))
  
  # Chunk into batches of 10 fields per embed/message
  batches <- split(fields_all, ceiling(seq_along(fields_all) / 10))
  
  sent <- 0L
  for (i in seq_along(batches)) {
    embed <- list(
      title = sprintf("New lines posted (%d of %d)", i, length(batches)),
      description = "Spread and Total picks for games where a line just became available.",
      color = 0x3498db,
      fields = batches[[i]]
    )
    ok <- send_discord(webhook, embeds = list(embed))
    if (ok) sent <- sent + length(batches[[i]])
    Sys.sleep(0.2)
  }
  
  # Mark these specific (id, field) as seen so we don't alert again
  stage_name <- DBI::Id(schema = NULL, table = "GamePicksLineSeen_stage")
  if (DBI::dbExistsTable(con, stage_name)) DBI::dbRemoveTable(con, stage_name)
  DBI::dbWriteTable(
    con, stage_name,
    new_rows %>% dplyr::mutate(first_value = as.numeric(line)) %>% dplyr::select(id, field, first_value),
    temporary = TRUE, overwrite = TRUE
  )
  
  sql_insert_seen <- '
    INSERT INTO "GamePicksLineSeen" (id, field, first_value, price_spread, price_over, price_under)
    SELECT id, field, first_value, NULL, NULL, NULL
    FROM "GamePicksLineSeen_stage"
    ON CONFLICT (id, field) DO NOTHING;
  '
  DBI::dbWithTransaction(con, {
    DBI::dbExecute(con, sql_insert_seen)
    try(DBI::dbRemoveTable(con, stage_name), silent = TRUE)
  })
  
  invisible(list(notified = sent, inserted = nrow(new_rows)))
}

# ------------------------------------------------------------------
# 4) Run
# ------------------------------------------------------------------
con <- dbConnect(
  RPostgres::Postgres(),
  dbname   = "neondb",
  host     = "ep-tiny-fog-aetzb4mp-pooler.c-2.us-east-2.aws.neon.tech",
  port     = 5432,
  user     = "neondb_owner",
  password = "npg_1d0oXImKqyJv",
  sslmode  = "require"
)
stopifnot(DBI::dbIsValid(con))
res <- post_new_line_picks_to_discord(con, webhook = GAME_PICK_WEBHOOK)
print(res)







# suppressPackageStartupMessages({
#   library(DBI)
#   library(dplyr)
#   library(lubridate)
# })
# 
# stopifnot(exists("con") && DBI::dbIsValid(con))
# stopifnot(exists("final_2025_out"))
# 
# # ---- 1) Define required schema (column -> SQL type) ----
# required_schema <- c(
#   id = "BIGINT",
#   startDate = "TIMESTAMPTZ",
#   season = "INTEGER",
#   week = "INTEGER",
#   team = "TEXT",
#   opponent = "TEXT",
#   pred_team_points = "DOUBLE PRECISION",
#   pred_opponent_points = "DOUBLE PRECISION",
#   pred_spread = "DOUBLE PRECISION",
#   best_book = "TEXT",
#   best_line = "DOUBLE PRECISION",
#   pick_margin = "DOUBLE PRECISION",
#   cover_margin = "DOUBLE PRECISION",
#   "\"Hit\"" = "BOOLEAN",
#   pred_total = "DOUBLE PRECISION",
#   actual_total = "DOUBLE PRECISION",
#   best_ou_book = "TEXT",
#   best_ou_line = "DOUBLE PRECISION",
#   ou_pick = "TEXT",
#   ou_pick_margin = "DOUBLE PRECISION",
#   ou_cover_margin = "DOUBLE PRECISION",
#   "\"ou_Hit\"" = "BOOLEAN",
#   spread_pick = "TEXT",
#   ou_pick_str = "TEXT",
#   spread_hit_probability = "DOUBLE PRECISION",
#   ou_hit_probability = "DOUBLE PRECISION",
#   spread_unit = "INTEGER",
#   ou_unit = "INTEGER",
#   dk_formatted_spread = "DOUBLE PRECISION",
#   dk_formatted_spread_price = "DOUBLE PRECISION",
#   fd_formatted_spread = "DOUBLE PRECISION",
#   fd_formatted_spread_price = "DOUBLE PRECISION",
#   mgm_formatted_spread = "DOUBLE PRECISION",
#   mgm_formatted_spread_price = "DOUBLE PRECISION",
#   espnbet_formatted_spread = "DOUBLE PRECISION",
#   espnbet_formatted_spread_price = "DOUBLE PRECISION",
#   dk_formatted_overunder = "DOUBLE PRECISION",
#   dk_formatted_total_over_price = "DOUBLE PRECISION",
#   dk_formatted_total_under_price = "DOUBLE PRECISION",
#   fd_formatted_overunder = "DOUBLE PRECISION",
#   fd_formatted_total_over_price = "DOUBLE PRECISION",
#   fd_formatted_total_under_price = "DOUBLE PRECISION",
#   mgm_formatted_overunder = "DOUBLE PRECISION",
#   mgm_formatted_total_over_price = "DOUBLE PRECISION",
#   mgm_formatted_total_under_price = "DOUBLE PRECISION",
#   espnbet_formatted_overunder = "DOUBLE PRECISION",
#   espnbet_formatted_total_over_price = "DOUBLE PRECISION",
#   espnbet_formatted_total_under_price = "DOUBLE PRECISION"
# )
# 
# # ---- 2) Ensure table exists & schema is up to date ----
# gp_exists <- DBI::dbExistsTable(con, DBI::Id(schema = "public", table = "GamePicks"))
# 
# if (!gp_exists) {
#   # Create fresh with full schema & PK on id
#   cols_sql <- paste(paste0('"', names(required_schema), '" ', unname(required_schema)), collapse = ",\n    ")
#   create_sql <- sprintf('CREATE TABLE "GamePicks" (%s, PRIMARY KEY ("id"));', cols_sql)
#   DBI::dbExecute(con, create_sql)
# } else {
#   # Add any missing columns
#   existing_cols <- DBI::dbGetQuery(
#     con,
#     "SELECT column_name FROM information_schema.columns
#      WHERE table_schema='public' AND table_name='GamePicks';"
#   )$column_name
#   for (nm in names(required_schema)) {
#     bare_nm <- gsub('^"|"$', "", nm)  # remove quotes in our names for lookup
#     if (!(bare_nm %in% existing_cols)) {
#       DBI::dbExecute(con, sprintf('ALTER TABLE "GamePicks" ADD COLUMN IF NOT EXISTS "%s" %s;', bare_nm, required_schema[[nm]]))
#     }
#   }
# }
# 
# # ---- 3) Align dataframe to table schema ----
# tbl_cols <- DBI::dbGetQuery(
#   con,
#   "SELECT column_name
#      FROM information_schema.columns
#     WHERE table_schema = 'public' AND table_name = 'GamePicks'
#     ORDER BY ordinal_position"
# )$column_name
# 
# df_to_write <- final_2025_out
# 
# # Add missing columns as NA
# missing_cols <- setdiff(tbl_cols, names(df_to_write))
# if (length(missing_cols)) for (cc in missing_cols) df_to_write[[cc]] <- NA
# 
# # Drop extras & reorder
# extra_cols <- setdiff(names(df_to_write), tbl_cols)
# if (length(extra_cols)) df_to_write <- dplyr::select(df_to_write, -dplyr::all_of(extra_cols))
# df_to_write <- dplyr::select(df_to_write, dplyr::all_of(tbl_cols))
# 
# # ---- 4) Coerce types (guard startDate) ----
# # If startDate exists and is not POSIXct, parse it; otherwise leave as-is
# if ("startDate" %in% names(df_to_write) && !inherits(df_to_write$startDate, "POSIXt")) {
#   df_to_write$startDate <- suppressWarnings(lubridate::ymd_hms(df_to_write$startDate, tz = "UTC"))
# }
# 
# df_to_write <- df_to_write %>%
#   mutate(
#     id            = as.numeric(id),   # BIGINT (numeric ok)
#     season        = as.integer(season),
#     week          = as.integer(week),
#     team          = as.character(team),
#     opponent      = as.character(opponent),
#     best_book     = as.character(best_book),
#     best_ou_book  = as.character(best_ou_book),
#     ou_pick       = as.character(ou_pick),
#     spread_pick   = as.character(spread_pick),
#     ou_pick_str   = as.character(ou_pick_str),
#     `Hit`         = as.logical(`Hit`),
#     ou_Hit        = as.logical(ou_Hit)
#   )
# 
# # ---- 5) TRUNCATE and INSERT ----
# DBI::dbWithTransaction(con, {
#   DBI::dbExecute(con, 'TRUNCATE TABLE "GamePicks";')
#   DBI::dbAppendTable(con, DBI::Id(schema = "public", table = "GamePicks"), df_to_write)
# })
# 
# # optional sanity check
# DBI::dbGetQuery(con, 'SELECT COUNT(*) FROM "GamePicks";')
