library(glmmTMB)
library(dplyr)
library(stringr) 
library(DBI)
library(RPostgres)
library(qs)

con <- dbConnect(
  RPostgres::Postgres(),
  dbname   = "neondb",
  host     = "ep-tiny-fog-aetzb4mp-pooler.c-2.us-east-2.aws.neon.tech",
  port     = 5432,
  user     = "neondb_owner",
  password = "npg_1d0oXImKqyJv",  # or Sys.getenv("NEON_PG_PASS")
  sslmode  = "require"            # keep this
)

model_df <- dbGetQuery(con, 'SELECT * FROM "PreparedData" WHERE season <= 2020;')

model_df <- model_df %>%
  mutate(
    season_num = numFactor(season),
    week_num = numFactor(week),
    team_f     = factor(team),
    opponent_f = factor(opponent),
    passing_plays = offense_passingPlays.totalPPA / offense_passingPlays.ppa, 
    rushing_plays = offense_rushingPlays.totalPPA / offense_rushingPlays.ppa, 
    passing_rate = passing_plays / (passing_plays + rushing_plays),
    points_above_average = offense_ppa * 65,
    passing_points_above_average = offense_passingPlays.ppa * 65,
    rushing_points_above_average = offense_rushingPlays.ppa * 65,
    is_home = ifelse(homeTeam == team, 1, 0)
    )%>%
  group_by(season) %>%
  mutate(
    team_talent_scaled     = as.numeric(scale(team_talent)),
    opponent_talent_scaled = as.numeric(scale(opponent_talent))
  ) %>%
  ungroup()

fit_overall <- glmmTMB(
  points_above_average ~ 0 + 
    is_home + 
    team_talent_scaled +
    opponent_talent_scaled +
    ou(season_num + 0 | team_f) + 
    ou(season_num + 0 | opponent_f) +
    ou(week_num + 0 | team_f:season) + 
    ou(week_num + 0 | opponent_f:season), 
  data = model_df, 
  family = gaussian(), 
  REML = FALSE,
)

summary(fit_overall)

fit_passing <- glmmTMB(
  passing_points_above_average ~ 0 +
    is_home + 
    team_talent_scaled +
    opponent_talent_scaled +
    ou(season_num + 0 | team_f) + 
    ou(season_num + 0 | opponent_f) + 
    ou(week_num + 0 | team_f:season) + 
    ou(week_num + 0 | opponent_f:season), 
  data = model_df, 
  family = gaussian(), 
  REML = FALSE)

summary(fit_passing)

fit_rushing <- glmmTMB(
  rushing_points_above_average ~ 0 +
    is_home + 
    team_talent_scaled +
    opponent_talent_scaled +
    ou(season_num + 0 | team_f) + 
    ou(season_num + 0 | opponent_f) + 
    ou(week_num + 0 | team_f:season) +
    ou(week_num + 0 | opponent_f:season),
  data = model_df, 
  family = gaussian(), 
  REML = FALSE)

summary(fit_rushing)

if (!dir.exists("glmms")) {
  dir.create("glmms")
}

qsave(fit_overall, "glmms/fit_overall.qs")
qsave(fit_passing, "glmms/fit_passing.qs")
qsave(fit_rushing, "glmms/fit_rushing.qs")

team_f_levels <- levels(droplevels(model_df$team_f))
write.csv(data.frame(team = team_f_levels), "glmms/team_f_levels.csv", row.names = FALSE)
