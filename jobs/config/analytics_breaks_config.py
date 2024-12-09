from data_processing.data_aggregations import (
    calculate_breaks_team_stats_shares_from_agg,
    aggregate_breaks_team_stats_from_raw,
)
from models.analytics.breaks import Break, BreaksTeamStats, BreaksTeamStatsShares
from models.data.fixtures import Fixture

ANALYTICS_BREAKS_ENTITIES_CONFIG = {
    "breaks": {
        "get_method": Fixture.get_breaks,
        "dependencies": [],
        "upsert_method": Break.upsert,
    },
    "breaks_team_stats": {
        "get_method": Break.get_breaks_team_stats_raw,
        "dependencies": [aggregate_breaks_team_stats_from_raw],
        "upsert_method": BreaksTeamStats.upsert,
    },
    # "breaks_team_stats_share": {
    #     "get_method": calculate_breaks_team_stats_shares_from_agg,
    #     "dependencies": [],
    #     "upsert_method": BreaksTeamStatsShares.upsert,
    # },
}
