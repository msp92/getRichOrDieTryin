import logging
from typing import List
from models.data_warehouse.main import Team, Country
from services.api.generic_fetcher import GenericFetcher


def pull_coaches_for_all_teams():
    """
    Example job that fetches all teams from DB, then pulls coaches for each team.
    """
    fetcher = GenericFetcher()
    teams_df = Team.get_df_from_table()
    team_ids = [str(x) for x in teams_df["team_id"]]  # ensure strings
    fetcher.pull_data_for_list(
        values=team_ids,
        endpoint_template="coaches?team={}",  # in old code: "couchs?team={}" or similar
        filename_prefix="COACH_TEAM_",
        subdir="coaches",
    )


def pull_fixtures_by_dates(dates_to_pull: List[str]) -> None:
    if not dates_to_pull:
        logging.info("No dates to update for fixtures.")
        return
    fetcher = GenericFetcher()
    fetcher.pull_data_by_dates(
        dates=dates_to_pull,
        endpoint_template="fixtures?date={}",
        filename_prefix="FIXTURES_",
        subdir="fixtures",
    )


def pull_events_by_dates(dates_to_pull: List[str]) -> None:
    if not dates_to_pull:
        logging.info("No dates to update for fixture events.")
        return
    fetcher = GenericFetcher()
    fetcher.pull_data_by_dates(
        dates=dates_to_pull,
        endpoint_template="fixtures/events?fixture={}",
        filename_prefix="FIXTURE_EVENTS_",
        subdir="fixture_events",
    )


def pull_fixtures_stats_by_dates(dates_to_pull: List[str]) -> None:
    if not dates_to_pull:
        logging.info("No dates to update for player stats.")
        return
    fetcher = GenericFetcher()
    fetcher.pull_data_by_dates(
        dates=dates_to_pull,
        endpoint_template="fixtures/stats?fixture={}",
        filename_prefix="FIXTURE_STATS_",
        subdir="fixture_stats",
    )


def pull_player_stats_by_dates(dates_to_pull: List[str]) -> None:
    if not dates_to_pull:
        logging.info("No dates to update for player stats.")
        return
    fetcher = GenericFetcher()
    fetcher.pull_data_by_dates(
        dates=dates_to_pull,
        endpoint_template="fixtures/players?fixture={}",
        filename_prefix="FIXTURE_PLAYER_STATS_",
        subdir="fixture_player_stats",
    )


def pull_teams_for_all_countries():
    fetcher = GenericFetcher()
    all_countries = Country.get_df_from_table()
    fetcher.pull_data_for_list(
        values=all_countries["country_name"].dropna().tolist(),
        endpoint_template="teams?country={}",
        filename_prefix="TEAMS_",
        subdir="teams",
    )


def pull_fixture_stats_by_ids(fixture_ids: list[int]):
    if not fixture_ids:
        logging.info("No fixtures to update for fixture stats.")
        return
    fetcher = GenericFetcher()
    string_ids = [str(x) for x in fixture_ids]
    fetcher.pull_data_for_list(
        values=string_ids,
        endpoint_template="fixtures/statistics?fixture={}",
        filename_prefix="FIXTURE_STATS_",
        subdir="fixtures_stats",
    )
