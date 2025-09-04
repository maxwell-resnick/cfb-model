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

connect_neon <- function() {
  url <- Sys.getenv("DATABASE_URL", unset = "")
  if (!nzchar(url)) stop("DATABASE_URL is not set")
  
  # Poolers can choke on this param; strip it if present
  url <- sub("[&?]channel_binding=[^&]+", "", url)
  
  u <- httr::parse_url(url)
  if (is.null(u$hostname)) stop("Malformed DATABASE_URL")
  
  dbname <- if (nzchar(u$path)) sub("^/", "", u$path) else ""
  host   <- u$hostname
  port   <- if (is.null(u$port)) 5432 else u$port
  user   <- u$username
  pass   <- u$password
  sslm   <- if (!is.null(u$query$sslmode)) u$query$sslmode else "require"
  
  # IMPORTANT: no 'options=' here (pooler forbids it)
  con <- DBI::dbConnect(
    RPostgres::Postgres(),
    dbname   = dbname,
    host     = host,
    port     = port,
    user     = user,
    password = pass,
    sslmode  = sslm
  )
  
  # Optional: set search_path at session level (allowed; ignore if restricted)
  try(DBI::dbExecute(con, 'SET search_path TO public'), silent = TRUE)
  
  # Smoke test
  DBI::dbGetQuery(con, "SELECT 1 AS ok;")
  con
}

Sys.setenv(
  DATABASE_URL = sprintf(
    "postgresql://%s:%s@%s:%d/%s?sslmode=require",
    "neondb_owner",
    utils::URLencode("npg_POip9LKGFAa6", reserved = TRUE),  # URL-encode just in case
    "ep-tiny-fog-aetzb4mp-pooler.c-2.us-east-2.aws.neon.tech",
    5432,
    "neondb"
  )
)

con <- connect_neon()

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

source("./kalman_filter.R")
pregame_latents <- get_pregame_latents()

avg_row <- function(...) {
  x <- rowMeans(cbind(...), na.rm = TRUE)
  ifelse(is.nan(x), NA_real_, x)
}

# --- helper used below (safe row average) ---
if (!exists("avg_row", mode = "function")) {
  avg_row <- function(...) {
    x <- rowMeans(cbind(...), na.rm = TRUE)
    ifelse(is.nan(x), NA_real_, x)
  }
}

# --- choose columns that might carry opening/current OU info ---
ou_open_cols <- intersect(
  c(
    "bovada_formatted_opening_overunder",
    "espnbet_formatted_opening_overunder",  # keep/remove as you wish
    # use current books as fallback signal for opening OU level
    "dk_formatted_overunder",
    "fd_formatted_overunder",
    "mgm_formatted_overunder"
  ),
  names(spread_scores_keys_2025)
)

ou_curr_cols <- intersect(
  c(
    "dk_formatted_overunder",
    "fd_formatted_overunder",
    "mgm_formatted_overunder",
    "bovada_formatted_overunder",
    "espnbet_formatted_overunder"
  ),
  names(spread_scores_keys_2025)
)

# --- compute GLOBAL averages once (numeric, ignoring non-finite) ---
global_open_ou_avg <- {
  vals <- unlist(lapply(ou_open_cols, function(nm)
    suppressWarnings(as.numeric(spread_scores_keys_2025[[nm]]))
  ), use.names = FALSE)
  vals <- vals[is.finite(vals)]
  if (length(vals)) mean(vals) else NA_real_
}

global_curr_ou_avg <- {
  vals <- unlist(lapply(ou_curr_cols, function(nm)
    suppressWarnings(as.numeric(spread_scores_keys_2025[[nm]]))
  ), use.names = FALSE)
  vals <- vals[is.finite(vals)]
  if (length(vals)) mean(vals) else NA_real_
}

# --- build formatted_* and then IMPUTE with the GLOBAL avg(s) ---
spread_scores_keys_2025 <- spread_scores_keys_2025 %>%
  mutate(
    # OPENING lines (your existing construction)
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
    
    # >>> GLOBAL IMPUTE: only when spread exists but opening OU is missing
    formatted_opening_overunder = if_else(
      is.na(formatted_opening_overunder) & !is.na(formatted_opening_spread),
      global_open_ou_avg,
      formatted_opening_overunder
    ),
    
    # CURRENT lines (your existing construction)
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
    
    # (Optional) GLOBAL IMPUTE for current OU too — comment out if not desired
    formatted_overunder = if_else(
      is.na(formatted_overunder),
      global_curr_ou_avg,
      formatted_overunder
    ),
    
    # Team-point calcs (keep your sign convention)
    vegas_opening_team_points = (formatted_opening_overunder + formatted_opening_spread) / 2,
    vegas_team_points         = (formatted_overunder         + formatted_spread)         / 2
  )


con <- connect_neon()

passing_rate_df <- dbGetQuery(con, 'SELECT * FROM "PreparedData" WHERE season >= 2023;')

avg3 <- function(a, b, c) {
  n <- (!is.na(a)) + (!is.na(b)) + (!is.na(c))
  (coalesce(a, 0) + coalesce(b, 0) + coalesce(c, 0)) / ifelse(n == 0, NA_real_, n)
}

passing_rate_df <- passing_rate_df %>%
  mutate(
    is_home = ifelse(homeTeam == team, 1, 0),
    bovada_formatted_opening_spread = case_when(
    is_home == 1L ~ -bovada_opening_spread,
    is_home == 0L ~  bovada_opening_spread,
    TRUE          ~ NA_real_
    ),
    espnbet_formatted_opening_spread = case_when(
      is_home == 1L ~ -espnbet_opening_spread,
      is_home == 0L ~  espnbet_opening_spread,
      TRUE          ~ NA_real_
    ),
    draftkings_formatted_opening_spread = case_when(
      is_home == 1L ~ -draftkings_opening_spread,
      is_home == 0L ~  draftkings_opening_spread,
      TRUE          ~ NA_real_
    ),
    formatted_opening_spread = avg3(
      bovada_formatted_opening_spread, espnbet_formatted_opening_spread, draftkings_formatted_opening_spread
    )
  )

passing_rate_df <- passing_rate_df %>%
  mutate(
    passing_plays = suppressWarnings(as.numeric(`offense_passingPlays.totalPPA`) /
                                       as.numeric(`offense_passingPlays.ppa`)),
    rushing_plays = suppressWarnings(as.numeric(`offense_rushingPlays.totalPPA`) /
                                       as.numeric(`offense_rushingPlays.ppa`)),
    total_plays   = suppressWarnings(as.numeric(passing_plays + rushing_plays)),
    passing_rate  = passing_plays / (passing_plays + rushing_plays),
    
    startDate     = as.POSIXct(startDate, tz = "UTC"),
    formatted_opening_spread = suppressWarnings(as.numeric(formatted_opening_spread)),
    is_completed  = !is.na(homePoints) & !is.na(awayPoints)
  )

lm_pr <- lm(
  passing_rate ~ formatted_opening_spread,
  data = passing_rate_df %>% filter(is_completed, !is.na(passing_rate), !is.na(formatted_opening_spread))
)
lm_tp <- lm(
  total_plays ~ formatted_opening_spread,
  data = passing_rate_df %>% filter(is_completed, !is.na(total_plays), !is.na(formatted_opening_spread))
)

print(summary(lm_pr))
print(summary(lm_tp))

# 3) Vectorized predictions and residuals (completed games only)
b_pr <- coef(lm_pr); b_tp <- coef(lm_tp)
b0_pr <- unname(b_pr["(Intercept)"]);                  b1_pr <- unname(b_pr["formatted_opening_spread"])
b0_tp <- unname(b_tp["(Intercept)"]);                  b1_tp <- unname(b_tp["formatted_opening_spread"])

passing_rate_df <- passing_rate_df %>%
  mutate(
    pr_pred = b0_pr + b1_pr * formatted_opening_spread,
    tp_pred = b0_tp + b1_tp * formatted_opening_spread,
    pr_resid = if_else(is_completed & !is.na(passing_rate), passing_rate - pr_pred, NA_real_),
    tp_resid = if_else(is_completed & !is.na(total_plays),  total_plays  - tp_pred, NA_real_)
  )

# 4) Per-team most-recent 12-game rolling residual means (exclude current via lag)
rolling_passing_rate_summary <- passing_rate_df %>%
  arrange(team, startDate) %>%
  group_by(team) %>%
  mutate(
    rolling_passing_rate = slide_dbl(
      lag(pr_resid),
      .f = function(x) {
        x <- x[!is.na(x)]
        if (length(x) < 12) return(NA_real_)
        mean(tail(x, 12))
      },
      .before = Inf, .complete = FALSE
    ),
    rolling_total_plays = slide_dbl(
      lag(tp_resid),
      .f = function(x) {
        x <- x[!is.na(x)]
        if (length(x) < 12) return(NA_real_)
        mean(tail(x, 12))
      },
      .before = Inf, .complete = FALSE
    )
  ) %>%
  slice_tail(n = 1) %>%                      # keep most recent row per team
  ungroup() %>%
  select(team, rolling_passing_rate, rolling_total_plays)

combined_2025 <- pregame_latents %>%
  dplyr::filter(season == 2025) %>%
  dplyr::left_join(spread_scores_keys_2025, by = c("season","week","team","opponent")) %>%
  # avoid duplicate column name clashes if these already exist
  dplyr::select(-dplyr::any_of(c("rolling_passing_rate","rolling_total_plays"))) %>%
  dplyr::left_join(rolling_passing_rate_summary, by = "team")


swap_cols <- c("offense_effect_overall","offense_effect_passing","offense_effect_rushing",
               "defense_effect_overall","defense_effect_passing","defense_effect_rushing",
               "rolling_passing_rate", "rolling_total_plays")

opp_feats_2025 <- combined_2025 %>%
  dplyr::select(id, team, opponent, season, week, dplyr::all_of(swap_cols)) %>%
  dplyr::rename_with(~ paste0("opponent_", .), .cols = dplyr::all_of(swap_cols)) %>%
  dplyr::rename(team_swapped = opponent, opponent_swapped = team)

combined_2025 <- combined_2025 %>%
  dplyr::left_join(
    opp_feats_2025,
    by = c("id","season","week","team" = "team_swapped","opponent" = "opponent_swapped")
  )

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

# ---- Predictions for team points (already have final_xgb_fit, features_used, combined_2025) ----
pred_2025 <- combined_2025 %>%
  mutate(across(all_of(features_used), as.numeric),
         vegas_opening_team_points = vegas_team_points)

X_2025 <- as.matrix(pred_2025[, features_used, drop = FALSE])
d2025  <- xgboost::xgb.DMatrix(X_2025, missing = NA)
pred_2025$pred_team_points <- predict(final_xgb_fit, d2025)

# Opponent predictions + team-perspective margin (FIXED SIGN)
opp_preds_2025 <- pred_2025 %>%
  select(id, team, pred_team_points) %>%
  rename(opponent = team, pred_opponent_points = pred_team_points)

pred_2025 <- pred_2025 %>%
  left_join(opp_preds_2025, by = c("id", "opponent")) %>%
  mutate(
    pred_margin = pred_team_points - pred_opponent_points,  # team - opp (team-perspective)
    pred_total  = pred_team_points + pred_opponent_points,
    actual_total = if_else(!is.na(team_points) & !is.na(opponent_points),
                           team_points + opponent_points, NA_real_)
  )

# Ensure optional columns exist (so matrices below never fail)
ensure_cols <- function(df, cols, default = NA_real_) {
  for (nm in cols) if (!nm %in% names(df)) df[[nm]] <- default
  df
}

pred_2025 <- ensure_cols(
  pred_2025,
  c("dk_formatted_spread","fd_formatted_spread","mgm_formatted_spread","espnbet_formatted_spread",
    "dk_formatted_spread_price","fd_formatted_spread_price","mgm_formatted_spread_price","espnbet_formatted_spread_price",
    "dk_formatted_overunder","fd_formatted_overunder","mgm_formatted_overunder","espnbet_formatted_overunder",
    "dk_formatted_total_over_price","fd_formatted_total_over_price","mgm_formatted_total_over_price","espnbet_formatted_total_over_price",
    "dk_formatted_total_under_price","fd_formatted_total_under_price","mgm_formatted_total_under_price","espnbet_formatted_total_under_price")
)

# Small epsilon for push checks
if (!exists("eps", inherits = TRUE)) eps <- 1e-3

# -----------------------------
# Vectorized SPREAD pick (DK / FD / MGM / ESPN)
# -----------------------------
book_names  <- c("draftkings","fanduel","betmgm","espnbet")

L_spread <- as.matrix(pred_2025[, c("dk_formatted_spread","fd_formatted_spread","mgm_formatted_spread","espnbet_formatted_spread")])
P_spread <- as.matrix(pred_2025[, c("dk_formatted_spread_price","fd_formatted_spread_price","mgm_formatted_spread_price","espnbet_formatted_spread_price")])

n <- nrow(pred_2025)
if (is.null(dim(L_spread))) L_spread <- matrix(L_spread, nrow = n)   # guard for 1-row edge case
if (is.null(dim(P_spread))) P_spread <- matrix(P_spread, nrow = n)

PM <- matrix(pred_2025$pred_margin, nrow = n, ncol = ncol(L_spread))
E_spread <- PM - L_spread                                      # edge = predicted team margin - line
E_spread[is.na(E_spread)] <- -Inf

# best edge per row
Emax <- apply(E_spread, 1, max)
has_line <- is.finite(Emax)

# tie-break by price: only among cols achieving Emax (within tol)
tol <- 1e-12
mask_best <- sweep(E_spread, 1, Emax - tol, FUN = ">=") & is.finite(E_spread)
P_masked  <- ifelse(mask_best, P_spread, NA_real_)

# idx by price if any, else by edge
idx_edge  <- ifelse(has_line, max.col(replace(E_spread, !is.finite(E_spread), -Inf), ties.method = "first"), NA_integer_)
has_price <- rowSums(!is.na(P_masked)) > 0
idx_price <- ifelse(has_price, max.col(replace(P_masked, is.na(P_masked), -Inf), ties.method = "first"), NA_integer_)
idx_best  <- ifelse(is.na(idx_price), idx_edge, idx_price)  # prefer price tie-break when available

best_line_spread <- rep(NA_real_, n)
best_edge_spread <- rep(NA_real_, n)
best_book_spread <- rep(NA_character_, n)

ok <- !is.na(idx_best)
if (any(ok)) {
  rr <- which(ok)
  cc <- idx_best[ok]
  best_line_spread[ok] <- L_spread[cbind(rr, cc)]
  best_edge_spread[ok] <- E_spread[cbind(rr, cc)]
  best_book_spread[ok] <- book_names[cc]
}

pred_2025 <- pred_2025 %>%
  mutate(
    best_book  = best_book_spread,
    best_line  = best_line_spread,
    pick_margin = best_edge_spread,                       # = pred_margin - best_line
    cover_margin = ifelse(!is.na(score_diff) & !is.na(best_line), score_diff - best_line, NA_real_),
    is_push_actual = ifelse(!is.na(cover_margin), abs(cover_margin) <= eps, NA),
    is_push_pick   = ifelse(!is.na(pick_margin),  abs(pick_margin)  <= eps, NA),
    Hit = case_when(
      is.na(cover_margin) | is.na(is_push_pick) ~ NA,
      is_push_actual | is_push_pick             ~ NA,
      TRUE                                      ~ sign(cover_margin) == sign(pick_margin)
    )
  )

# Choose the single spread pick per game (the team with the bigger modeled edge)
picks_2025 <- pred_2025 %>%
  select(
    id, startDate, season, week, team, opponent,
    team_points, opponent_points, score_diff,
    pred_team_points, pred_opponent_points, pred_margin,
    best_book, best_line, pick_margin, cover_margin, Hit,
    dk_formatted_spread,  dk_formatted_spread_price,
    fd_formatted_spread,  fd_formatted_spread_price,
    mgm_formatted_spread, mgm_formatted_spread_price,
    espnbet_formatted_spread, espnbet_formatted_spread_price
  ) %>%
  group_by(id) %>%
  slice_max(order_by = pick_margin, n = 1, with_ties = FALSE) %>%
  ungroup()

stopifnot(anyDuplicated(picks_2025$id) == 0)

# -----------------------------
# Vectorized TOTAL pick (DK / FD / MGM / ESPN)
# -----------------------------
L_total <- as.matrix(pred_2025[, c("dk_formatted_overunder","fd_formatted_overunder","mgm_formatted_overunder","espnbet_formatted_overunder")])
PO      <- as.matrix(pred_2025[, c("dk_formatted_total_over_price","fd_formatted_total_over_price",
                                   "mgm_formatted_total_over_price","espnbet_formatted_total_over_price")])
PU      <- as.matrix(pred_2025[, c("dk_formatted_total_under_price","fd_formatted_total_under_price",
                                   "mgm_formatted_total_under_price","espnbet_formatted_total_under_price")])

if (is.null(dim(L_total))) L_total <- matrix(L_total, nrow = n)
if (is.null(dim(PO)))      PO      <- matrix(PO, nrow = n)
if (is.null(dim(PU)))      PU      <- matrix(PU, nrow = n)

PT <- matrix(pred_2025$pred_total, nrow = n, ncol = ncol(L_total))
E_over  <- PT - L_total                     # edge if betting OVER
E_under <- L_total - PT                     # edge if betting UNDER
E_over[is.na(E_over)]   <- -Inf
E_under[is.na(E_under)] <- -Inf

Eo_max <- apply(E_over,  1, max)
Eu_max <- apply(E_under, 1, max)
pick_over <- Eo_max >= Eu_max               # choose direction by larger edge

# function to choose best book with price tiebreak for a given edge matrix & price matrix
pick_idx_with_price <- function(E, P) {
  Emax <- apply(E, 1, max)
  has_line <- is.finite(Emax)
  mask_best <- sweep(E, 1, Emax - tol, FUN = ">=") & is.finite(E)
  Pm <- ifelse(mask_best, P, NA_real_)
  idx_edge  <- ifelse(has_line, max.col(replace(E, !is.finite(E), -Inf), ties.method = "first"), NA_integer_)
  has_price <- rowSums(!is.na(Pm)) > 0
  idx_price <- ifelse(has_price, max.col(replace(Pm, is.na(Pm), -Inf), ties.method = "first"), NA_integer_)
  ifelse(is.na(idx_price), idx_edge, idx_price)
}

idx_over  <- pick_idx_with_price(E_over,  PO)
idx_under <- pick_idx_with_price(E_under, PU)
idx_tot   <- ifelse(pick_over, idx_over, idx_under)

best_ou_book <- rep(NA_character_, n)
best_ou_line <- rep(NA_real_, n)
ou_pick      <- ifelse(pick_over, "over", "under")
# rows where we actually have a total line
has_any_total <- rowSums(is.finite(E_over) | is.finite(E_under)) > 0 & !is.na(idx_tot)

if (any(has_any_total)) {
  rr <- which(has_any_total)
  cc <- idx_tot[has_any_total]
  best_ou_book[has_any_total] <- book_names[cc]
  best_ou_line[has_any_total] <- L_total[cbind(rr, cc)]
}

# OU margins + hit
pred_2025 <- pred_2025 %>%
  mutate(
    best_ou_book = best_ou_book,
    best_ou_line = best_ou_line,
    ou_pick      = ifelse(!has_any_total, NA, ou_pick),
    ou_pick_margin = case_when(
      ou_pick == "over"  ~ pred_total - best_ou_line,
      ou_pick == "under" ~ best_ou_line - pred_total,
      TRUE ~ NA_real_
    ),
    ou_cover_margin = case_when(
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

# ---------- helpers (idempotent) ----------
if (!exists("fmt_line", mode = "function")) {
  fmt_line <- function(x) ifelse(
    is.na(x), NA_character_,
    ifelse(x > 0, paste0("+", format(round(x, 1), nsmall = 1)),
           format(round(x, 1), nsmall = 1))
  )
}
if (!exists("calc_units", mode = "function")) {
  calc_units <- function(p) {
    ifelse(
      is.na(p),
      NA_integer_,
      {
        # 0 if < 52.5%
        # 1 for [0.53,0.55), 2 for [0.55,0.57), 3 for [0.57,0.59),
        # 4 for [0.59,0.61), 5 for >= 0.61
        n <- floor((p - 0.53) / 0.02) + 1L
        ifelse(p < 0.525, 0L, pmin(5L, pmax(0L, n)))
      }
    )
  }
}
if (!exists("load_units_line", mode = "function")) {
  load_units_line <- function(path) {
    obj <- qs::qread(path)
    if (is.list(obj) && all(c("m","b") %in% names(obj))) {
      return(list(m = as.numeric(obj$m), b = as.numeric(obj$b)))
    }
    if (is.numeric(obj) && length(obj) >= 2) {
      return(list(m = as.numeric(obj[1]), b = as.numeric(obj[2])))
    }
    if (is.character(obj) && length(obj) == 1) {
      m <- suppressWarnings(as.numeric(stringr::str_match(obj, "y\\s*=\\s*([+-]?[0-9\\.eE]+)\\s*\\*\\s*x")[,2]))
      b <- suppressWarnings(as.numeric(stringr::str_match(obj, "\\+\\s*([+-]?[0-9\\.eE]+)\\s*$")[,2]))
      if (!is.na(m) && !is.na(b)) return(list(m = m, b = b))
    }
    stop(sprintf("Could not parse units line from %s", path))
  }
}

# Load calibration coefs if not already present
if (!exists("spread_coefs", inherits = TRUE)) spread_coefs <- load_units_line("spread_units_line.qs")
if (!exists("ou_coefs",     inherits = TRUE)) ou_coefs     <- load_units_line("ou_units_line.qs")

# ---------- bring per-book TOTALS columns from pred_2025 ----------
ou_cols <- c(
  "dk_formatted_overunder","fd_formatted_overunder",
  "mgm_formatted_overunder","espnbet_formatted_overunder",
  "dk_formatted_total_over_price","fd_formatted_total_over_price",
  "mgm_formatted_total_over_price","espnbet_formatted_total_over_price",
  "dk_formatted_total_under_price","fd_formatted_total_under_price",
  "mgm_formatted_total_under_price","espnbet_formatted_total_under_price"
)
for (nm in setdiff(ou_cols, names(pred_2025))) pred_2025[[nm]] <- NA_real_

# ---------- build final_2025_out with probabilities & units ----------
final_2025_out <- picks_2025 %>%
  # add totals market lines/prices for the chosen team row
  dplyr::left_join(
    pred_2025 %>% dplyr::select(id, team, dplyr::all_of(ou_cols)),
    by = c("id","team")
  ) %>%
  # join OU pick summary (already resolved to one row per id)
  dplyr::left_join(
    picks_totals_2025 %>%
      dplyr::select(
        id, best_ou_book, best_ou_line, ou_pick,
        ou_pick_margin, ou_cover_margin, ou_Hit,
        pred_total, actual_total
      ),
    by = "id"
  ) %>%
  # pretty strings
  mutate(best_line = -best_line,
      dk_formatted_spread = -dk_formatted_spread,
      fd_formatted_spread = -fd_formatted_spread,
      mgm_formatted_spread = -mgm_formatted_spread,
      espnbet_formatted_spread = -espnbet_formatted_spread,
      spread_pick = dplyr::if_else(
      is.na(team) | is.na(best_line), NA_character_,
      paste0(team, " ", fmt_line(best_line))
    ),
    ou_pick_str = dplyr::if_else(
      is.na(ou_pick) | is.na(best_ou_line), NA_character_,
      paste0(stringr::str_to_title(ou_pick), " ", format(round(best_ou_line, 1), nsmall = 1))
    )
  ) %>%
  # probabilities from calibration lines
  dplyr::mutate(
    spread_hit_probability = pmin(
      pmax(spread_coefs$m * as.numeric(pick_margin) + spread_coefs$b, 0), 1
    ),
    ou_hit_probability = pmin(
      pmax(ou_coefs$m * as.numeric(ou_pick_margin) + ou_coefs$b, 0), 1
    ),
    spread_unit = calc_units(spread_hit_probability),
    ou_unit     = calc_units(ou_hit_probability)
  ) %>%
  dplyr::select(
    id, startDate, season, week, team, opponent,
    pred_team_points, pred_opponent_points, pred_margin,
    best_book, best_line, pick_margin, cover_margin, Hit, spread_pick,
    pred_total, actual_total, best_ou_book, best_ou_line,
    ou_pick, ou_pick_margin, ou_cover_margin, ou_Hit, ou_pick_str,
    spread_hit_probability, ou_hit_probability, spread_unit, ou_unit,
    # spreads per-book
    dk_formatted_spread,      dk_formatted_spread_price,
    fd_formatted_spread,      fd_formatted_spread_price,
    mgm_formatted_spread,     mgm_formatted_spread_price,
    espnbet_formatted_spread, espnbet_formatted_spread_price,
    # totals per-book
    dplyr::all_of(ou_cols)
  ) %>%
  dplyr::arrange(season, week, team)


suppressPackageStartupMessages({
  library(dplyr)
  library(lubridate)
  library(gt)
  library(webshot2) # gtsave backend
})

# ------------------------------------------------------------------
# 0) Webhook setup (fallback to provided URL if the env var is empty)
# ------------------------------------------------------------------
GAME_PICK_WEBHOOK <- Sys.getenv("GAME_PICK_WEBHOOK", unset = "")
if (!nzchar(GAME_PICK_WEBHOOK)) {
  GAME_PICK_WEBHOOK <- "https://discord.com/api/webhooks/1410096344093167777/uvLW1ZSOs0oEqCmAye8GaRKE3cCyVJj2bYFxgeEehiRCkTZnRsZe6CWczxKA7cwU8Bul"
}

send_df_as_png <- function(webhook, df, title = NULL, file_path = "tmp_table.png") {
  if (nrow(df) == 0) return(FALSE)
  
  tbl_gt <- df %>%
    gt() %>%
    tab_header(title = title) %>%
    opt_all_caps(FALSE) %>%
    fmt_number(
      columns = everything(),
      decimals = 3
    )
  
  # save as PNG
  gtsave(tbl_gt, file_path, expand = 10) # expand ensures no clipping
  
  # send to Discord as file
  payload <- httr::POST(
    url = webhook,
    httr::add_headers(`Content-Type` = "multipart/form-data"),
    body = list(file = httr::upload_file(file_path))
  )
  
  sc <- httr::status_code(payload)
  isTRUE(sc %in% c(200, 204))
}

post_window_picks_to_discord_png <- function(final_2025_out,
                                             webhook = Sys.getenv("GAME_PICK_WEBHOOK", unset = ""),
                                             days_min = 3L, days_max = 10L,
                                             tz = "UTC") {
  if (!nzchar(webhook)) return(invisible(FALSE))
  
  now_utc <- lubridate::now(tzone = tz)
  min_dt  <- now_utc + days(days_min)
  max_dt  <- now_utc + days(days_max)
  
  df <- final_2025_out %>%
    mutate(startDate = as.POSIXct(startDate, tz = tz, origin = "1970-01-01")) %>%
    filter(startDate >= min_dt, startDate <= max_dt)
  
  # Spread picks, descending by spread_hit_probability
  spreads <- df %>%
    filter(!is.na(best_book), spread_unit >= 1) %>%
    select(team, opponent, best_book, spread_pick, spread_hit_probability, spread_unit) %>%
    arrange(desc(spread_hit_probability))
  
  # Totals picks, descending by ou_hit_probability
  totals <- df %>%
    filter(!is.na(best_ou_book), ou_unit >= 1) %>%
    select(team, opponent, best_ou_book, ou_pick_str, ou_hit_probability, ou_unit) %>%
    arrange(desc(ou_hit_probability))
  
  window_str <- paste0(
    format(min_dt, "%Y-%m-%d %H:%M", tz = tz), " → ",
    format(max_dt, "%Y-%m-%d %H:%M", tz = tz), " (", tz, ")"
  )
  
  sent1 <- sent2 <- FALSE
  if (nrow(spreads)) {
    sent1 <- send_df_as_png(
      webhook,
      spreads,
      title = paste0("Spread picks (units ≥ 1) — window: ", window_str),
      file_path = "spreads_table.png"
    )
  }
  if (nrow(totals)) {
    sent2 <- send_df_as_png(
      webhook,
      totals,
      title = paste0("Totals picks (units ≥ 1) — window: ", window_str),
      file_path = "totals_table.png"
    )
  }
  
  # fallback message if nothing qualifies
  if (!sent1 && !sent2) {
    msg <- paste0(
      "**No qualifying picks** in window ", window_str,
      " (spreads: units ≥ 1 & book present; totals: units ≥ 1 & book present)."
    )
    isTRUE(send_discord(webhook, content = msg))
  }
  
  invisible(list(spreads = nrow(spreads), totals = nrow(totals)))
}

post_window_picks_to_discord_png(final_2025_out, webhook = GAME_PICK_WEBHOOK)


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
