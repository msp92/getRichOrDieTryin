from config.api_config import ApiConfig
from data_processing.data_parsing import parse_seasons, parse_leagues, parse_countries
from models.data.main import Country, League, Season
from services.countries_fetcher import CountryFetcher
from services.leagues_fetcher import LeagueFetcher

api_config = ApiConfig()

leagues_fetcher = LeagueFetcher(config=api_config)
countries_fetcher = CountryFetcher(config=api_config)

data = countries_fetcher.get_countries()
countries_fetcher.write_response_to_json(data, "countries")
data = leagues_fetcher.get_leagues()
leagues_fetcher.write_response_to_json(data, "leagues")

### Countries ###
df_countries = parse_countries()
Country.upsert(df_countries)

### Leagues ###
df_leagues = parse_leagues()
League.upsert(df_leagues)

### Seasons ###
df_seasons = parse_seasons()
Season.upsert(df_seasons)
