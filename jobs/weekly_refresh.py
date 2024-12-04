import logging

from config.api_config import ApiConfig
from config.entity_names import COUNTRIES_FILE_NAME, LEAGUES_FILE_NAME
from data_processing.data_parsing import (
    parse_seasons,
    parse_leagues,
    parse_countries,
    parse_teams,
)
from models.data.main import Country, League, Season, Team
from services.countries_fetcher import CountryFetcher
from services.leagues_fetcher import LeagueFetcher
from services.teams_fetcher import TeamFetcher

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

api_config = ApiConfig()

# Initialize API fetchers
leagues_fetcher = LeagueFetcher(config=api_config)
countries_fetcher = CountryFetcher(config=api_config)
teams_fetcher = TeamFetcher(config=api_config)


def main():
    # Fetch data from API
    countries_data = countries_fetcher.fetch_all_countries()
    countries_fetcher.write_response_to_json(countries_data, COUNTRIES_FILE_NAME)

    leagues_data = leagues_fetcher.fetch_all_leagues()
    leagues_fetcher.write_response_to_json(leagues_data, LEAGUES_FILE_NAME)

    # Parse input data and upsert to tables
    countries_df = parse_countries()
    Country.upsert(countries_df)

    leagues_df = parse_leagues()
    League.upsert(leagues_df)

    seasons_df = parse_seasons()
    Season.upsert(seasons_df)

    teams_fetcher.fetch_all_teams()

    teams_df = parse_teams()
    Team.upsert(teams_df)


if __name__ == main():
    main()
