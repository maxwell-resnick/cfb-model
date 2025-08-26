# pip install cfbd pandas numpy
import os, re, itertools
import io, json
import pandas as pd
import numpy as np
import cfbd
from pandas import json_normalize
import ssl
from sqlalchemy import create_engine, text, inspect
import os, certifi
os.environ["SSL_CERT_FILE"] = certifi.where()
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()

# =========================
# Config
# =========================
YEAR = 2025
BEARER = os.environ.get("BEARER_TOKEN")
assert BEARER, "Set env var BEARER_TOKEN with your CollegeFootballData API token"
configuration = cfbd.Configuration(access_token=BEARER)
configuration.verify_ssl = True
configuration.ssl_ca_cert = certifi.where()

# =========================
# Helpers
# =========================
def normalize_team_col(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure a 'team' string column exists (some CFBD endpoints use 'school')."""
    if "team" not in df.columns and "school" in df.columns:
        df = df.rename(columns={"school": "team"})
    return df

def impute_line_values_refactor(
    lines_df: pd.DataFrame,
    overwrite: bool = False,
    consensus_name: str = "consensus",
) -> pd.DataFrame:
    """Pivot provider lines and compute non-consensus medians / ranges."""
    df = lines_df.copy()
    if df.empty:
        return df

    # numeric coercions
    for c in ("spread", "spreadOpen", "overUnder", "overUnderOpen"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    def _slug(s: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(s).lower())

    def _canonical_provider(p: str) -> str:
        s = _slug(p)
        aliases = {
            "draftkingssportsbook": "draftkings",
            "draftkings": "draftkings",
            "dk": "draftkings",
            "bovada": "bovada",
            "espn": "espnbet",
            "espnbet": "espnbet",
            "consensus": "consensus",
        }
        return aliases.get(s, s)

    df["provider_clean"] = df.get("provider", "consensus")
    df["provider_clean"] = df["provider_clean"].map(_canonical_provider)

    # pivot helper
    def _pivot(values_col: str, suffix: str) -> pd.DataFrame:
        if values_col not in df.columns:
            return pd.DataFrame()
        p = (
            df.pivot_table(
                index="id",
                columns="provider_clean",
                values=values_col,
                aggfunc="median",
                observed=True,
            )
            .rename_axis(None, axis=1)
        )
        p.columns = [f"{c}_{suffix}" for c in p.columns]
        return p.reset_index()

    pivots = [
        _pivot("spread", "spread"),
        _pivot("spreadOpen", "opening_spread"),
        _pivot("overUnder", "overunder"),
        _pivot("overUnderOpen", "opening_overunder"),
    ]
    base = pd.DataFrame({"id": df["id"].unique()})
    for p in pivots:
        if not p.empty:
            base = base.merge(p, on="id", how="left")

    # line move (close - open) per provider
    if {"spread", "spreadOpen"}.issubset(df.columns):
        g = (
            df.groupby(["id", "provider_clean"], observed=True)[["spread", "spreadOpen"]]
              .median()
              .reset_index()
        )
        g["line_move"] = g["spread"] - g["spreadOpen"]
        lm = g.pivot(index="id", columns="provider_clean", values="line_move").reset_index()
        lm.columns = ["id"] + [f"{c}_line_move" for c in lm.columns if c != "id"]
        base = base.merge(lm, on="id", how="left")

    # non-consensus median + range
    non_consensus_mask = df["provider_clean"].ne(_canonical_provider(consensus_name))
    aggs = (
        df[non_consensus_mask]
        .groupby("id", as_index=False, observed=True)
        .agg(
            spread_imputed=("spread", "median"),
            overUnder_imputed=("overUnder", "median"),
            min_spread=("spread", "min"),
            max_spread=("spread", "max"),
        )
    )

    out = (
        df.drop(columns=[c for c in ["provider_clean"] if c in df.columns])
          .merge(base, on="id", how="left")
          .merge(aggs, on="id", how="left")
    )

    # overwrite with medians if requested
    if overwrite:
        if {"spread", "spread_imputed"}.issubset(out.columns):
            out["spread"] = np.where(out["spread_imputed"].notna(), out["spread_imputed"], out["spread"])
        if {"overUnder", "overUnder_imputed"}.issubset(out.columns):
            out["overUnder"] = np.where(out["overUnder_imputed"].notna(), out["overUnder_imputed"], out["overUnder"])

    return out

# =========================
# Fetch 2025 games
# =========================
with cfbd.ApiClient(configuration) as api_client:
    games_api = cfbd.GamesApi(api_client)
    print(f"Fetching games for {YEAR} ...")
    games = games_api.get_games(year=YEAR)
    games_raw = [g.to_dict() for g in games]
    games_df = pd.json_normalize(games_raw, sep="_")
    games_df["season"] = YEAR

games_df.rename(
    columns={
        "home_id": "homeId",
        "home_team": "homeTeam",
        "home_points": "homePoints",
        "home_classification": "homeClassification",
        "home_conference": "homeConference",
        "away_id": "awayId",
        "away_team": "awayTeam",
        "away_points": "awayPoints",
        "away_classification": "awayClassification",
        "away_conference": "awayConference",
        "start_date": "startDate",
        "season_type": "seasonType",
        "conference_game": "conferenceGame",
        "neutral_site": "neutralSite",
    },
    inplace=True,
)

# =========================
# Fetch 2025 advanced game stats
# =========================
with cfbd.ApiClient(configuration) as api_client:
    stats_api = cfbd.StatsApi(api_client)
    print(f"Fetching advanced stats for {YEAR} ...")
    rsp = stats_api.get_advanced_game_stats(year=YEAR)
    mdf = pd.DataFrame([r.to_dict() for r in rsp])

if not mdf.empty:
    off = pd.json_normalize(mdf["offense"]).add_prefix("offense_")
    deff = pd.json_normalize(mdf["defense"]).add_prefix("defense_")
    metrics_df = pd.concat([mdf.drop(columns=["offense", "defense"]), off, deff], axis=1)
    if "game_id" in metrics_df.columns and "gameId" not in metrics_df.columns:
        metrics_df.rename(columns={"game_id": "gameId"}, inplace=True)
    metrics_df["season"] = YEAR
    metrics_df = normalize_team_col(metrics_df)
else:
    metrics_df = pd.DataFrame(columns=["gameId", "season", "team"])

# =========================
# Fetch 2025 team talent
# =========================
with cfbd.ApiClient(configuration) as api_client:
    teams_api = cfbd.TeamsApi(api_client)
    print(f"Fetching team talent for {YEAR} ...")
    t_rsp = teams_api.get_talent(year=YEAR)
    talent_df = pd.DataFrame([t.to_dict() for t in t_rsp])
    talent_df = normalize_team_col(talent_df)
    talent_df.rename(columns={"year": "season"}, inplace=True, errors="ignore")
    if "season" not in talent_df.columns:
        talent_df["season"] = YEAR

# =========================
# Fetch 2025 lines
# =========================
with cfbd.ApiClient(configuration) as api_client:
    betting_api = cfbd.BettingApi(api_client)
    print(f"Fetching betting lines for {YEAR} ...")
    l_rsp = betting_api.get_lines(year=YEAR)
    lines_df = pd.DataFrame([l.to_dict() for l in l_rsp])
    lines_df["year"] = YEAR

if not lines_df.empty:
    lines_exploded = lines_df.explode("lines", ignore_index=True)
    line_details = pd.json_normalize(lines_exploded["lines"])
    lines_df = pd.concat([lines_exploded.drop(columns=["lines"]), line_details], axis=1)
    if "week_x" in lines_df.columns and "week" not in lines_df.columns:
        lines_df.rename(columns={"week_x": "week"}, inplace=True)
    lines_df = impute_line_values_refactor(lines_df, overwrite=True)
else:
    lines_df = pd.DataFrame(columns=["id", "provider", "spread", "spreadOpen", "overUnder", "overUnderOpen", "week", "year"])

# =========================
# Fetch 2025 returning production
# =========================
with cfbd.ApiClient(configuration) as api_client:
    players_api = cfbd.PlayersApi(api_client)
    print(f"Fetching returning production for {YEAR} ...")
    rp_rsp = players_api.get_returning_production(year=YEAR)
    rp_df = pd.DataFrame([r.to_dict() for r in rp_rsp])

if not rp_df.empty:
    returning_df = pd.json_normalize(rp_df.to_dict(orient="records"))
    returning_df = normalize_team_col(returning_df)
    if "year" in returning_df.columns and "season" not in returning_df.columns:
        returning_df.rename(columns={"year": "season"}, inplace=True)
    if "season" not in returning_df.columns:
        returning_df["season"] = YEAR
else:
    returning_df = pd.DataFrame(columns=["season", "team", "conference"])

# =========================
# De-dupe basics
# =========================
if "id" in games_df.columns:
    games_df = games_df.drop_duplicates(subset=["id"])
if not metrics_df.empty:
    key_m = "gameId" if "gameId" in metrics_df.columns else ("id" if "id" in metrics_df.columns else None)
    if key_m:
        metrics_df = metrics_df.drop_duplicates(subset=[key_m, "team"] if "team" in metrics_df.columns else [key_m])
if not talent_df.empty and "team" in talent_df.columns:
    talent_df = talent_df.drop_duplicates(subset=["season", "team"])
if not returning_df.empty and "team" in returning_df.columns:
    returning_df = returning_df.drop_duplicates(subset=["season", "team"])
if not lines_df.empty and "id" in lines_df.columns:
    lines_df = lines_df.drop_duplicates(subset=["id"])

# =========================
# Make long schedule: 2 rows per game from games_api
# =========================
core_cols = [
    "id","season","week","seasonType","startDate","neutralSite","conferenceGame",
    "homeId","homeTeam","homeClassification","homeConference","homePoints",
    "awayId","awayTeam","awayClassification","awayConference","awayPoints",
]
for c in core_cols:
    if c not in games_df.columns:
        games_df[c] = pd.NA

home_rows = games_df[core_cols].copy()
home_rows["team"]     = games_df["homeTeam"]
home_rows["opponent"] = games_df["awayTeam"]
home_rows["is_home"]  = 1

away_rows = games_df[core_cols].copy()
away_rows["team"]     = games_df["awayTeam"]
away_rows["opponent"] = games_df["homeTeam"]
away_rows["is_home"]  = 0

games_long = pd.concat([home_rows, away_rows], ignore_index=True)

# =========================
# MERGES (all LEFT joins off full schedule) — season-safe
# =========================
# 1) + advanced metrics (team-level)
if "gameId" not in metrics_df.columns and "id" in metrics_df.columns:
    metrics_df = metrics_df.rename(columns={"id": "gameId"})

metrics_keys_left  = ["id", "team"]
metrics_keys_right = ["gameId", "team"]
if ("season" in metrics_df.columns) and ("season" in games_long.columns):
    metrics_keys_left.append("season")
    metrics_keys_right.append("season")

have_metrics_keys = all(k in metrics_df.columns for k in metrics_keys_right)

if have_metrics_keys and not metrics_df.empty:
    merge1 = pd.merge(
        games_long,
        metrics_df,
        how="left",
        left_on=metrics_keys_left,
        right_on=metrics_keys_right,
    ).drop(columns=["gameId"], errors="ignore")
else:
    merge1 = games_long.copy()

# Safety: restore plain 'season' if suffixed
if "season" not in merge1.columns:
    if "season_x" in merge1.columns:
        merge1.rename(columns={"season_x": "season"}, inplace=True)
    elif "season_y" in merge1.columns:
        merge1.rename(columns={"season_y": "season"}, inplace=True)
    else:
        merge1["season"] = YEAR

# 2) + talent (team-season)
if {"season","team","talent"}.issubset(talent_df.columns) and not talent_df.empty:
    merge2 = pd.merge(
        merge1,
        talent_df[["season","team","talent"]],
        how="left",
        on=["season","team"],
    )
else:
    merge2 = merge1.copy()

# 3) + returning production (team-season)
if {"season","team"}.issubset(returning_df.columns) and not returning_df.empty:
    merge3 = pd.merge(
        merge2,
        returning_df,
        how="left",
        on=["season","team"],
    )
else:
    merge3 = merge2.copy()

# 4) + lines (game-level -> attach to both team rows)
if "id" in lines_df.columns and not lines_df.empty:
    merge4 = pd.merge(
        merge3,
        lines_df,
        how="left",
        left_on="id",
        right_on="id",
    )
else:
    merge4 = merge3.copy()

final_df = merge4

# =========================
# Clean .x/.y duplicate suffixes
# =========================
all_cols = final_df.columns.tolist()
x_suffixes = [c for c in all_cols if c.endswith((".x","_x"))]
y_suffixes = [c for c in all_cols if c.endswith((".y","_y"))]
bases = set(re.sub(r"[._](x|y)$", "", c) for c in itertools.chain(x_suffixes, y_suffixes))

for base in bases:
    cx1, cx2 = f"{base}.x", f"{base}_x"
    cy1, cy2 = f"{base}.y", f"{base}_y"
    if cx1 in final_df.columns or cx2 in final_df.columns:
        final_df.drop(columns=[c for c in [cy1, cy2] if c in final_df.columns], inplace=True, errors="ignore")
        for c in [cx1, cx2]:
            if c in final_df.columns:
                final_df.rename(columns={c: base}, inplace=True)
    elif cy1 in final_df.columns or cy2 in final_df.columns:
        for c in [cy1, cy2]:
            if c in final_df.columns:
                final_df.rename(columns={c: base}, inplace=True)

final_df = final_df.loc[:, ~final_df.columns.duplicated()].copy()

# =========================
# Build df2025 (keep dynamic stats but will enforce base schema later)
# =========================
df2025 = final_df.copy()
df2025 = df2025[df2025.get("season", YEAR).eq(YEAR)].copy()

print("df2025 rows (should be 2x number_of_games):", len(df2025))

# =========================
# past_data.csv -> align & concat (ensure df2025 adopts any past_df-only cols as NA)
# =========================
past_path = "past_data.csv"
try:
    past_df = pd.read_csv(past_path)

    # normalize id if past_data used gameId
    if "id" not in past_df.columns and "gameId" in past_df.columns:
        past_df["id"] = past_df["gameId"]

    # add any columns in past_df that are missing in df2025 (filled NA)
    missing_cols = [c for c in past_df.columns if c not in df2025.columns]
    for c in missing_cols:
        df2025[c] = pd.NA

    # reorder df2025 so past_df columns come first, keep extras at end
    ordered_cols = list(past_df.columns) + [c for c in df2025.columns if c not in past_df.columns]
    df2025 = df2025.reindex(columns=ordered_cols)

    combined_df = pd.concat([past_df, df2025], ignore_index=True, sort=False)
    print("combined_df shape:", combined_df.shape)
except FileNotFoundError:
    combined_df = df2025.copy()
    print("past_data.csv not found; combined_df = df2025")

cols_to_keep_base = [
    "id","season","week","seasonType","startDate","neutralSite","conferenceGame",
    "team","opponent","homeId","homeTeam","homeClassification","homeConference","homePoints",
    "awayId","awayTeam","awayClassification","awayConference","awayPoints",
    "talent","percentPPA","provider","min_spread","max_spread",
    "offense_ppa", 
    "offense_passingPlays.totalPPA","offense_passingPlays.ppa",
    "offense_rushingPlays.totalPPA","offense_rushingPlays.ppa",
    "home_field_indicator",
    "bovada_spread","bovada_opening_spread","bovada_overunder","bovada_opening_overunder",
    "draftkings_spread","draftkings_opening_spread","draftkings_overunder","draftkings_opening_overunder",
    "espnbet_spread","espnbet_opening_spread","espnbet_overunder","espnbet_opening_overunder",
]


# reindex will add any missing base cols (as NA) and drop all non-base cols
df2025      = df2025.reindex(columns=cols_to_keep_base)
combined_df = combined_df.reindex(columns=cols_to_keep_base)

def build_model_data(final_df: pd.DataFrame) -> pd.DataFrame:
    """
    Pandas port of your R cleaning pipeline (keeps played + unplayed games).
    Returns a modeling DataFrame; advanced-stat fields remain NA when unavailable.

    NOTE: This version keeps ONLY FBS vs FBS games (drops any game where either side is not exactly 'fbs').
    """
    df = final_df.copy()

    # ensure 'opponent' exists
    if "opponent" not in df.columns:
        df["opponent"] = np.nan

    # base filter (exclude spring)
    df = df[(df["seasonType"].isna()) | (df["seasonType"] != "spring_regular")].copy()

    # is_home
    def _norm(series):
        out = series.astype(str).str.strip().str.lower()
        return out.mask(out.isin(["nan", "none", "null"]), np.nan)

    df["is_home"] = (_norm(df["team"]) == _norm(df["homeTeam"])).astype("Int64")

    # ids and classifications
    df["homeId"] = pd.to_numeric(df.get("homeId"), errors="coerce")
    df["awayId"] = pd.to_numeric(df.get("awayId"), errors="coerce")
    df["team_id"]     = df["homeId"].where(df["is_home"].eq(1), df["awayId"]).astype("Int64")
    df["opponent_id"] = df["awayId"].where(df["is_home"].eq(1), df["homeId"]).astype("Int64")

    df["team_classification"]     = df["homeClassification"].where(df["is_home"].eq(1), df["awayClassification"])
    df["team_conference"]         = df["homeConference"].where(df["is_home"].eq(1),   df["awayConference"])
    df["opponent_classification"] = df["awayClassification"].where(df["is_home"].eq(1), df["homeClassification"])
    df["opponent_conference"]     = df["awayConference"].where(df["is_home"].eq(1),    df["homeConference"])

    # opponent fallback
    df["opponent"] = df["opponent"].where(
        ~(df["opponent"].isna() | (df["opponent"].astype(str).str.len() == 0)),
        df["awayTeam"].where(df["is_home"].eq(1), df["homeTeam"])
    )

    # times
    df["start_dt"] = pd.to_datetime(df["startDate"], errors="coerce", utc=True)
    df["season"]   = pd.to_numeric(df["season"], errors="coerce").astype("Int64")

    # game day index within season
    df["game_day_index"] = (
        df.groupby("season")["start_dt"]
          .transform(lambda s: (s - s.min()).dt.days + 1)
          .astype("Int64")
    )

    # service academy talent patch
    svc_mask = (df["season"] >= 2016) & df["team"].isin(["Army", "Navy", "Air Force"]) & (df["talent"].isna() | (df["talent"] == 0))
    df.loc[svc_mask, "talent"] = 500

    # team-season lookups
    talent_map = (
        df.dropna(subset=["team","season","talent"])
          .sort_values(["season","team"])
          .drop_duplicates(subset=["season","team"])
          [["season","team","talent"]]
    )
    percent_map = (
        df.dropna(subset=["team","season","percentPPA"])
          .sort_values(["season","team"])
          .drop_duplicates(subset=["season","team"])
          [["season","team","percentPPA"]]
    )

    df = df.merge(talent_map.rename(columns={"talent":"team_talent"}),
                  on=["season","team"], how="left")
    df = df.merge(talent_map.rename(columns={"team":"opponent","talent":"opponent_talent"}),
                  on=["season","opponent"], how="left")
    df = df.merge(percent_map.rename(columns={"percentPPA":"team_percentPPA"}),
                  on=["season","team"], how="left")
    df = df.merge(percent_map.rename(columns={"team":"opponent","percentPPA":"opponent_percentPPA"}),
                  on=["season","opponent"], how="left")

    # boolean-ish -> ints
    if "conferenceGame" in df.columns:
        cg = df["conferenceGame"]
        if cg.dtype == bool:
            df["conferenceGame"] = cg.astype(int)
        else:
            xs = cg.astype(str).str.lower()
            df["conferenceGame"] = xs.isin(["true","t","1","yes","y"]).astype(int)
    if "neutralSite" in df.columns:
        ns = df["neutralSite"]
        if ns.dtype == bool:
            df["neutralSite"] = ns.astype(int)
        else:
            xs = ns.astype(str).str.lower()
            df["neutralSite"] = xs.isin(["true","t","1","yes","y"]).astype(int)
    else:
        df["neutralSite"] = 0

    # ordering + indices
    df = df.sort_values(["season","team","start_dt"])
    df["week_index"]       = df.groupby(["season","team"]).cumcount() + 1
    df["team_game_number"] = df["week_index"]

    df = df.sort_values(["season","opponent","start_dt"])
    df["opponent_game_number"] = df.groupby(["season","opponent"]).cumcount() + 1

    df["team_season"]     = df["team"].astype(str) + ":" + df["season"].astype(str)
    df["opponent_season"] = df["opponent"].astype(str) + ":" + df["season"].astype(str)

    # KEEP ALL rows (played + unplayed) and tag them
    df["played"] = df["homePoints"].notna() & df["awayPoints"].notna()
    df["has_metrics"] = df["offense_ppa"].notna()

    # Ensure each game id appears exactly twice (home/away rows)
    if "id" in df.columns:
        cnt = df.groupby("id").size()
        if not (cnt == 2).all():
            raise AssertionError("Two rows per game id required.")

    # scale within-season (NA-safe)
    if "offense_ppa" in df.columns:
        df["offense_ppa_scaled"] = (
            df.groupby("season")["offense_ppa"]
              .transform(lambda s: (s - s.mean()) / (s.std(ddof=0) if s.std(ddof=0) else 1.0))
        )
    else:
        df["offense_ppa_scaled"] = np.nan

    # ---------------------------
    # KEEP ONLY FBS vs FBS games
    # ---------------------------
    cls_team = df["team_classification"].astype("string").str.strip().str.lower()
    cls_opp  = df["opponent_classification"].astype("string").str.strip().str.lower()
    df = df[cls_team.eq("fbs") & cls_opp.eq("fbs")].copy()

    # points and score diff
    df["team_points"]     = pd.to_numeric(df["homePoints"].where(df["is_home"].eq(1), df["awayPoints"]), errors="coerce")
    df["opponent_points"] = pd.to_numeric(df["awayPoints"].where(df["is_home"].eq(1), df["homePoints"]), errors="coerce")
    df["score_diff"]      = df["team_points"] - df["opponent_points"]

    # provider lines (Bovada only)
    prov_cols = ["bovada_spread","bovada_opening_spread","bovada_overunder","bovada_opening_overunder","consensus_spread"]
    for c in prov_cols:
        if c not in df.columns: df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["raw_spread"]                  = df["bovada_spread"]
    df["raw_opening_spread"]          = df["bovada_opening_spread"]
    df["formatted_overunder"]         = df["bovada_overunder"]
    df["formatted_opening_overunder"] = df["bovada_opening_overunder"]

    df["formatted_spread"]           = df["raw_spread"].where(df["is_home"].eq(0), -df["raw_spread"])
    df["formatted_opening_spread"]   = df["raw_opening_spread"].where(df["is_home"].eq(0), -df["raw_opening_spread"])
    df["formatted_consensus_spread"] = df["consensus_spread"].where(df["is_home"].eq(0), -df["consensus_spread"])
    df["formattedSpread"]            = df["formatted_spread"]

    # weeks (nullable int)
    df["week"] = pd.to_numeric(df.get("week"), errors="coerce").astype("Int64")

    # postseason renumbering
    post = df[df["seasonType"] == "postseason"].copy()
    if not post.empty:
        post = post.sort_values(["team","season","startDate"])
        post["new_week"] = post.groupby(["team","season"]).cumcount() + 1
        df = df.merge(post[["team","season","startDate","new_week"]],
                      on=["team","season","startDate"], how="left")
        mask_ps = df["seasonType"] == "postseason"
        df.loc[mask_ps, "week"] = pd.to_numeric(df.loc[mask_ps, "new_week"], errors="coerce").astype("Int64")
        df.drop(columns=["new_week"], inplace=True)

        maxw = (df.loc[df["seasonType"] == "regular"]
                  .groupby("season")["week"]
                  .max()
                  .rename("max_regular_week"))
        df = df.merge(maxw, on="season", how="left")
        mask_shift = mask_ps & (pd.to_numeric(df["week"], errors="coerce") <= 5)
        df.loc[mask_shift, "week"] = (
            pd.to_numeric(df.loc[mask_shift, "week"], errors="coerce")
            + pd.to_numeric(df.loc[mask_shift, "max_regular_week"], errors="coerce")
        ).astype("Int64")
        df.drop(columns=["max_regular_week"], inplace=True)

    # dedupe first game’s week within (team, season)
    df = df.sort_values(["team","season","startDate"])
    dup_mask = (df["week"] == 1) & df.duplicated(subset=["team","season","week"], keep="first")
    df.loc[dup_mask, "week"] = 0
    df["week"] = pd.to_numeric(df["week"], errors="coerce").fillna(0).astype(int) + 1

    # targets (leave NA when unplayed/no metrics)
    df["points_above_average"]         = pd.to_numeric(df.get("offense_ppa"), errors="coerce") * 65
    df["passing_points_above_average"] = pd.to_numeric(df.get("offense_passingPlays.ppa"), errors="coerce") * 65
    df["rushing_points_above_average"] = pd.to_numeric(df.get("offense_rushingPlays.ppa"), errors="coerce") * 65

    # home field indicator
    hfi = pd.Series(np.nan, index=df.index)
    hfi.loc[df["neutralSite"].eq(1)] = 0
    hfi.loc[(df["neutralSite"].eq(0)) & (df["is_home"].eq(1))] = 1
    hfi.loc[(df["neutralSite"].eq(0)) & (df["is_home"].eq(0))] = -1
    df["home_field_indicator"] = hfi.astype("Int64")

    # indices & integrity (after FBS-only filter)
    teams = sorted(pd.unique(pd.concat([df["team"], df["opponent"]], ignore_index=True).dropna()))
    idx_map = {t:i+1 for i,t in enumerate(teams)}
    df["team_idx"]     = pd.Series(df["team"].map(idx_map), index=df.index).astype("Int64")
    df["opponent_idx"] = pd.Series(df["opponent"].map(idx_map), index=df.index).astype("Int64")

    if "id" in df.columns:
        cnt = df.groupby("id").size()
        if not (cnt == 2).all():
            # It's okay if non-FBS games were removed entirely,
            # but every remaining id must still have exactly two rows.
            raise AssertionError("After FBS-only filter, each remaining game id must have two rows.")

    # opponent percentPPA imputation scaffold (kept but not applied)
    opp_pct_map = (df.groupby(["opponent","season"])["team_percentPPA"]
                     .mean(numeric_only=True)
                     .rename("opponent_percentPPA_imputed")
                     .reset_index()
                     .rename(columns={"opponent":"opponent_key"}))
    df = df.merge(opp_pct_map, left_on=["opponent","season"], right_on=["opponent_key","season"], how="left")
    df.drop(columns=["opponent_key","opponent_percentPPA_imputed"], inplace=True)

    # robust within-season ordering + dense week rank
    df = df.sort_values(["season","start_dt"])
    df["week_in_season"] = df.groupby("season")["week"].transform(lambda s: s.rank(method="dense")).astype("Int64")

    return df

model_df = build_model_data(combined_df)

TABLE = "PreparedData"
KEYS  = ["id", "team", "opponent"]

DATABASE_URL = (
    "postgresql+pg8000://neondb_owner:npg_1d0oXImKqyJv"
    "@ep-tiny-fog-aetzb4mp-pooler.c-2.us-east-2.aws.neon.tech/neondb"
)

# model_df must already exist at this point
# model_df = build_model_data(combined_df)

# ---------- CONNECT ----------
engine = create_engine(
   DATABASE_URL,
   pool_pre_ping=True,
   connect_args={"ssl_context": ssl.create_default_context()},
)

# ---------- OPTIONAL: ensure helper stamp column exists ----------
with engine.begin() as conn:
    conn.execute(text(
        'ALTER TABLE "PreparedData" '
        'ADD COLUMN IF NOT EXISTS date_updated timestamptz NOT NULL DEFAULT now();'
    ))

# ---------- Canonicalization rules (stable diffs) ----------
DEFAULT_TEXT_COLS = [
    "seasonType","startDate","team","opponent","homeTeam","awayTeam",
    "homeConference","awayConference","homeClassification","awayClassification","provider"
]
DEFAULT_DATE_COLS = ["startDate"]
DEFAULT_NUM_COLS = [
    "season","week","homePoints","awayPoints","talent","percentPPA","min_spread","max_spread",
    "offense_ppa","offense_passingPlays.totalPPA","offense_passingPlays.ppa",
    "offense_rushingPlays.totalPPA","offense_rushingPlays.ppa","home_field_indicator",
    "team_talent","opponent_talent",
    "bovada_spread","bovada_opening_spread","bovada_overunder","bovada_opening_overunder",
    "draftkings_spread","draftkings_opening_spread","draftkings_overunder","draftkings_opening_overunder",
    "espnbet_spread","espnbet_opening_spread","espnbet_overunder","espnbet_opening_overunder",
    "consensus_spread","consensus_overunder","consensus_opening_spread","consensus_opening_overunder",
    "neutralSite","conferenceGame","homeId","awayId","id"
]

IGNORE_FOR_DIFF = {"startDate"}

def canon(df_slice: pd.DataFrame) -> pd.DataFrame:
    x = df_slice.copy()

    # TEXT: trim, empty -> NA (use pandas string dtype for safe .str ops)
    for c in (set(DEFAULT_TEXT_COLS) & set(x.columns)):
        s = x[c].astype("string").str.strip()
        x[c] = s.replace({"": pd.NA})

    # DATES: to UTC -> epoch seconds (Int64)
    for c in (set(DEFAULT_DATE_COLS) & set(x.columns)):
        dt = pd.to_datetime(x[c], errors="coerce", utc=True)
        epoch = (dt.view("int64") // 1_000_000_000)
        x[c] = pd.Series(epoch).astype("Int64")

    # NUMERICS: coerce + round for stable diffs
    for c in (set(DEFAULT_NUM_COLS) & set(x.columns)):
        x[c] = pd.to_numeric(x[c], errors="coerce").round(6)

    return x

# ---------- 1) Discover DB columns & choose common cols ----------
with engine.connect() as conn:
    cols_in_db = {c["name"] for c in inspect(conn).get_columns(TABLE, schema="public")}

# Only work with intersection to avoid schema drift
common_cols = [c for c in model_df.columns if c in cols_in_db]
for k in KEYS:
    if k not in common_cols:
        common_cols.append(k)

# ---------- 2) Read ALL rows from DB for those columns ----------
read_cols = [c for c in common_cols if c in cols_in_db]
cols_sql  = ", ".join(f'"{c}"' for c in read_cols)

with engine.connect() as conn:
    db_df = pd.read_sql(text(f'SELECT {cols_sql} FROM "{TABLE}"'), conn)

# ---------- 3) Align frames ----------
model_trim = model_df[common_cols].copy()
db_trim    = db_df[[c for c in common_cols if c in db_df.columns]].copy() if not db_df.empty else pd.DataFrame(columns=common_cols)

# Index by composite key
model_idx = model_trim.set_index(KEYS, drop=False)
db_idx    = db_trim.set_index(KEYS, drop=False) if not db_trim.empty else db_trim

# ---------- 4) Detect NEW rows ----------
new_rows = model_trim.loc[~model_idx.index.isin(db_idx.index)].copy()

changed_rows = pd.DataFrame(columns=common_cols)
diff_summary = {}

if not db_trim.empty and not model_trim.empty:
    common_index = model_idx.index.intersection(db_idx.index)
    if len(common_index):
        nonkey_cols = [c for c in common_cols if c not in KEYS and c != "date_updated"]
        modN = canon(model_idx.loc[common_index, nonkey_cols])
        dbN  = canon(db_idx.loc[common_index, nonkey_cols])

        # cell-wise equality (after canon)
        eq = (modN.eq(dbN)) | (modN.isna() & dbN.isna())

        # --- count changes by column, excluding ignored columns (e.g., startDate)
        diff_summary = {c: int((~eq[c]).sum())
                        for c in modN.columns
                        if c not in IGNORE_FOR_DIFF}

        # --- treat ignored columns as equal so startDate-only diffs don't flag a row
        eq_ignore = eq.copy()
        for c in (set(IGNORE_FOR_DIFF) & set(eq.columns)):
            eq_ignore[c] = True

        # rows changed in at least one NON-ignored column
        changed_keys = eq_ignore.all(axis=1).index[~eq_ignore.all(axis=1)]
        if len(changed_keys):
            changed_rows = model_idx.loc[changed_keys, common_cols].copy()

print(f"NEW rows: {len(new_rows)}")
print(f"CHANGED rows (after normalization): {len(changed_rows)}")

if diff_summary:
    nz = {k: v for k, v in diff_summary.items() if v and k not in IGNORE_FOR_DIFF}
    if nz:
        print("Changed cells by column (non-zero only):", nz)

# --- NEW: print per-row diffs for Discord, ignoring startDate-only diffs ---
def _fmt(v):
    if pd.isna(v):
        return "NA"
    s = str(v).replace("\n", " ")
    return (s[:80] + "…") if len(s) > 81 else s

if not db_trim.empty and not model_trim.empty and 'eq' in locals():
    nonkey_cols = [c for c in common_cols if c not in KEYS and c != "date_updated"]
    # rows that differ in at least one column (by canonical comparison)
    changed_keys_idx = eq.all(axis=1).index[~eq.all(axis=1)]
    for idx in changed_keys_idx:
        # which columns changed (by canonical eq) and are not ignored
        ch_cols = [c for c in nonkey_cols
                   if (c in eq.columns and not eq.loc[idx, c] and c not in IGNORE_FOR_DIFF)]
        if not ch_cols:
            # only ignored columns (e.g., startDate) changed -> skip this row
            continue
        parts = []
        for c in ch_cols:
            old_v = db_idx.loc[idx, c] if c in db_idx.columns else pd.NA
            new_v = model_idx.loc[idx, c] if c in model_idx.columns else pd.NA
            parts.append(f"{c}: {_fmt(old_v)} -> {_fmt(new_v)}")
        gid, gteam, gopp = idx  # composite key
        print("DISCORD_DIFF", f"id={gid} team={gteam} opponent={gopp} | " + "; ".join(parts))

# Print NEW rows (keys only) so YAML can include them too
if not new_rows.empty:
    for r in new_rows.itertuples(index=False):
        print("DISCORD_NEW", f"id={getattr(r, 'id', 'NA')} team={getattr(r, 'team', 'NA')} opponent={getattr(r, 'opponent', 'NA')}")

# ---------- 6) UPSERT (INSERT new + UPDATE changed) ----------
to_upsert = pd.concat([new_rows, changed_rows], ignore_index=True).drop_duplicates(subset=KEYS)

if to_upsert.empty:
    print("No new/changed rows to upsert.")
else:
    cols_for_insert = [c for c in common_cols if c != "date_updated"]  # let DB default on INSERT
    update_cols     = [c for c in cols_for_insert if c not in KEYS]

    def _param_name(col: str) -> str:
        # safe param names for dotted columns
        return "p_" + re.sub(r"[^0-9a-zA-Z_]", "_", col)

    param_map = {c: _param_name(c) for c in cols_for_insert}
    cols_q = ", ".join(f'"{c}"' for c in cols_for_insert)
    vals_q = ", ".join(f':{param_map[c]}' for c in cols_for_insert)
    set_q  = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols] + ['"date_updated" = now()'])

    upsert_sql = text(f'''
        INSERT INTO "{TABLE}" ({cols_q})
        VALUES ({vals_q})
        ON CONFLICT ("id","team","opponent") DO UPDATE
        SET {set_q};
    ''')

    recs = []
    for _, row in to_upsert[cols_for_insert].iterrows():
        d = {}
        for c in cols_for_insert:
            v = row[c]
            d[param_map[c]] = None if pd.isna(v) else v
        recs.append(d)

    CHUNK = 500
    with engine.begin() as conn:
        for i in range(0, len(recs), CHUNK):
            conn.execute(upsert_sql, recs[i:i+CHUNK])

    print(f"Upserted rows: {len(to_upsert)}")

# ---------- 7) (Optional) Ensure PK exists on (id, team, opponent) ----------
with engine.begin() as conn:
    # add NOT NULLs first (safe if already set)
    conn.execute(text('ALTER TABLE "PreparedData" ALTER COLUMN "id"       SET NOT NULL;'))
    conn.execute(text('ALTER TABLE "PreparedData" ALTER COLUMN "team"     SET NOT NULL;'))
    conn.execute(text('ALTER TABLE "PreparedData" ALTER COLUMN "opponent" SET NOT NULL;'))

    # create PK if missing
    conn.execute(text('''
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint c
        JOIN pg_class t ON c.conrelid = t.oid
        JOIN pg_namespace n ON t.relnamespace = n.oid
        WHERE t.relname = 'PreparedData'
          AND n.nspname = 'public'
          AND c.contype = 'p'
      ) THEN
        ALTER TABLE "PreparedData"
        ADD CONSTRAINT prepareddata_pk PRIMARY KEY ("id","team","opponent");
      END IF;
    END$$;
    '''))

print("Sync complete.")