import logging

from config.api_config import ApiConfig
from data_processing.data_parsing import parse_seasons, parse_leagues, parse_countries
from models.data.main import Country, League, Season
from services.countries_fetcher import CountryFetcher
from services.leagues_fetcher import LeagueFetcher

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

api_config = ApiConfig()

# Initialize API fetchers
leagues_fetcher = LeagueFetcher(config=api_config)
countries_fetcher = CountryFetcher(config=api_config)

# Fetch data from API
countries_data = countries_fetcher.get_countries()
countries_fetcher.write_response_to_json(countries_data, "countries")

leagues_data = leagues_fetcher.get_leagues()
leagues_fetcher.write_response_to_json(leagues_data, "leagues")

# Parse input data and upsert to tables
countries_df = parse_countries()
Country.upsert(countries_df)

leagues_df = parse_leagues()
League.upsert(leagues_df)

seasons_df = parse_seasons()
Season.upsert(seasons_df)
