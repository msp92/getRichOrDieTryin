from config.entity_names import (
    FIXTURES_DIR,
    FIXTURE_STATS_DIR,
    FIXTURE_PLAYER_STATS_DIR,
    FIXTURE_EVENTS_DIR,
)
from data_processing.data_aggregations import aggregate_breaks_team_stats_from_raw
from data_processing.data_parsing import (
    parse_seasons,
    parse_leagues,
    parse_teams,
    parse_fixtures_file,
    parse_fixture_stats_file,
    parse_fixture_player_stats_file,
    parse_fixture_events_file,
)
from models.analytics.breaks import Break, BreaksTeamStats
from models.data_warehouse.main import Team, Season, League
from models.data_warehouse.fixtures import (
    Fixture,
    FixtureStat,
    FixturePlayerStat,
    FixtureEvent,
)
from services.api.fetch_jobs import (
    pull_fixtures_by_dates,
    pull_events_by_dates,
    pull_player_stats_by_dates,
    pull_fixtures_stats_by_dates,
    pull_teams_for_all_countries,
)
from services.api.generic_fetcher import GenericFetcher

MAIN_ENTITIES_CONFIG = {
    # "countries": {
    #     "api_pull_method": GenericFetcher().pull_single_endpoint("countries"),
    #     "parse_method": parse_countries,
    #     "upsert_method": Country.upsert,
    # },
    "leagues": {
        "api_pull_method": GenericFetcher().pull_single_endpoint("leagues"),
        "parse_method": parse_leagues,
        "upsert_method": League.upsert,
    },
    "seasons": {
        "api_pull_method": None,
        "parse_method": parse_seasons,
        "upsert_method": Season.upsert,
    },
    "teams": {
        "api_pull_method": pull_teams_for_all_countries,
        "parse_method": parse_teams,
        "upsert_method": Team.upsert,
    },
}

FIXTURE_ENTITIES_CONFIG = {
    "fixtures": {
        "dates_to_update_method": Fixture.get_dates_to_update,
        "api_pull_method": pull_fixtures_by_dates,
        "parse_method": parse_fixtures_file,
        "upsert_method": Fixture.upsert,
        "dependencies": [Team.insert_missing_teams_into_db],
        "input_dir": FIXTURES_DIR,
        "multiprocessing": False,
    },
    "fixture_events": {
        "dates_to_update_method": FixtureEvent.get_dates_to_update,
        "api_pull_method": pull_events_by_dates,
        "parse_method": parse_fixture_events_file,
        "upsert_method": FixtureEvent.upsert,
        "dependencies": [],
        "input_dir": FIXTURE_EVENTS_DIR,
        "multiprocessing": True,
    },
    "fixture_player_stats": {
        "dates_to_update_method": FixturePlayerStat.get_dates_to_update,
        "api_pull_method": pull_player_stats_by_dates,
        "parse_method": parse_fixture_player_stats_file,
        "upsert_method": FixturePlayerStat.upsert,
        "dependencies": [],
        "input_dir": FIXTURE_PLAYER_STATS_DIR,
        "multiprocessing": True,
    },
    "fixture_stats": {
        "dates_to_update_method": FixtureStat.get_dates_to_update,
        "api_pull_method": pull_fixtures_stats_by_dates,
        "parse_method": parse_fixture_stats_file,
        "upsert_method": FixtureStat.upsert,
        "dependencies": [],
        "input_dir": FIXTURE_STATS_DIR,
        "multiprocessing": True,
    },
}

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
}
