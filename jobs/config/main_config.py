from config.api_config import ApiConfig
from data_processing.data_parsing import (
    parse_seasons,
    parse_leagues,
    parse_teams,
    parse_coaches,
)
from models.data.main import Coach, Team, Season, League
from services import LeagueFetcher, TeamFetcher, CoachesFetcher

MAIN_ENTITIES_CONFIG = {
    # "countries": {
    #     "api_pull_method": CountryFetcher(config=ApiConfig()).fetch_all_countries,
    #     "parse_method": parse_countries,
    #     "upsert_method": Country.upsert,
    # },
    "leagues": {
        "api_pull_method": LeagueFetcher(config=ApiConfig()).fetch_all_leagues,
        "parse_method": parse_leagues,
        "upsert_method": League.upsert,
    },
    "seasons": {
        "api_pull_method": None,
        "parse_method": parse_seasons,
        "upsert_method": Season.upsert,
    },
    "teams": {
        "api_pull_method": TeamFetcher(config=ApiConfig()).fetch_all_teams,
        "parse_method": parse_teams,
        "upsert_method": Team.upsert,
    },
    "coaches": {
        "api_pull_method": CoachesFetcher(
            config=ApiConfig()
        ).pull_coaches_for_all_teams,
        "parse_method": parse_coaches,
        "upsert_method": Coach.upsert,
    },
}
