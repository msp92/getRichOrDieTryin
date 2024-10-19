import logging
import sys

from data_processing.data_aggregations import (
    aggregate_breaks_team_stats_from_raw,
    calculate_breaks_team_stats_shares_from_agg,
)
from models.analytics.breaks import Break, BreaksTeamStats, BreaksTeamStatsShares
from models.data.fixtures.fixtures import Fixture
from pathlib import Path

from config.api_config import ApiConfig
from services.fixtures_fetcher import FixtureFetcher

sys.path.append(str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

api_config = ApiConfig()
fixture_fetcher = FixtureFetcher(config=api_config)


# #
# fixture_fetcher.pull_finished_fixtures()
#
# ### Update specific tables with newly pulled data ###
#
# ### Fixtures ###
# df_updated_fixtures = parse_fixtures("updates")
# insert_missing_teams_into_db(df_updated_fixtures)
# Fixture.upsert(df_updated_fixtures)
# move_json_files_between_directories(
#     "/data/fixtures/updates", "/data/fixtures/updates/processed"
# )

### Fixture Stats ###

### Fixture Events ###

### Player Stats ###


### Breaks ###
df_breaks = Fixture.get_breaks()
Break.upsert(df_breaks)

## BreaksTeamStats ###
df_breaks_team_stats_raw = Break.get_breaks_team_stats_raw()
df_breaks_team_stats_agg_df = aggregate_breaks_team_stats_from_raw(
    df_breaks_team_stats_raw
)
BreaksTeamStats.upsert(df_breaks_team_stats_agg_df)
df_breaks_team_stats_shares_df = calculate_breaks_team_stats_shares_from_agg(
    df_breaks_team_stats_agg_df
)
BreaksTeamStatsShares.upsert(df_breaks_team_stats_shares_df)

### Expensive !!!
# breaks_with_factors_df = BreaksTeamStatsShares.get_breaks_with_factors()
# BreaksWithFactors.upsert(breaks_with_factors_df)
# FixturesWithFactors.upsert(breaks_with_factors_df)

### Fixture Events, Stats, Player Stats ###
# df_fixture_events = parse_events()
# FixtureEvent.upsert(df_fixture_events)
# df_fixture_stats = parse_fixture_stats()
# FixtureStat.upsert(df_fixture_stats)
# df_fixture_player_stats = parse_fixture_player_stats()
# FixturePlayerStat.upsert(df_fixture_player_stats)
