from config.api_config import ApiConfig
from config.entity_names import (
    FIXTURES_DIR,
    FIXTURE_STATS_DIR,
    FIXTURE_PLAYER_STATS_DIR,
    FIXTURE_EVENTS_DIR,
)
from data_processing.data_parsing import (
    parse_fixtures,
    parse_fixture_stats_file,
    parse_fixture_player_stats_file,
    parse_fixture_events_file,
)
from models.data.fixtures import (
    Fixture, FixtureStat, FixturePlayerStat, FixtureEvent
)
from models.data.main import Team
from services import (
    EventsFetcher, FixtureFetcher, PlayerStatsFetcher, StatsFetcher
)

FIXTURE_ENTITIES_CONFIG = {
    "fixtures": {
        "dates_to_update_method": Fixture.get_dates_to_update,
        "api_pull_method": FixtureFetcher(config=ApiConfig()).pull_fixtures_by_dates,
        "parse_method": parse_fixtures,
        "upsert_method": Fixture.upsert,
        "dependencies": [Team.insert_missing_teams_into_db],
        "input_dir": FIXTURES_DIR,
        "multiprocessing": False,
    },
    "fixture_events": {
        "dates_to_update_method": FixtureEvent.get_dates_to_update,
        "api_pull_method": EventsFetcher(config=ApiConfig()).pull_events_by_dates,
        "parse_method": parse_fixture_events_file,
        "upsert_method": FixtureEvent.upsert,
        "dependencies": [],
        "input_dir": FIXTURE_EVENTS_DIR,
        "multiprocessing": True,
    },
    "fixture_player_stats": {
        "dates_to_update_method": FixturePlayerStat.get_dates_to_update,
        "api_pull_method": PlayerStatsFetcher(config=ApiConfig()).pull_player_stats_by_dates,
        "parse_method": parse_fixture_player_stats_file,
        "upsert_method": FixturePlayerStat.upsert,
        "dependencies": [],
        "input_dir": FIXTURE_PLAYER_STATS_DIR,
        "multiprocessing": True,
    },
    "fixture_stats": {
        "dates_to_update_method": FixtureStat.get_dates_to_update,
        "api_pull_method": StatsFetcher(config=ApiConfig()).pull_stats_by_dates,
        "parse_method": parse_fixture_stats_file,
        "upsert_method": FixtureStat.upsert,
        "dependencies": [],
        "input_dir": FIXTURE_STATS_DIR,
        "multiprocessing": True,
    },
}
