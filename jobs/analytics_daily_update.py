import logging

from data_processing.data_aggregations import (
    calculate_breaks_team_stats_shares_from_agg,
    aggregate_breaks_team_stats_from_raw,
)
from models.analytics.breaks import Break, BreaksTeamStats, BreaksTeamStatsShares
from models.data.fixtures import Fixture


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main() -> None:
    # class AnalyticsPipeline
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


if __name__ == main():
    main()
