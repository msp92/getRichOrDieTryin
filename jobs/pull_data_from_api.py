import logging


from config.api_config import ApiConfig
from services.fixtures_fetcher import FixtureFetcher
from services.stats_fetcher import StatsFetcher
from services.teams_fetcher import TeamFetcher
from services.leagues_fetcher import LeagueFetcher
from services.countries_fetcher import CountryFetcher

# sys.path.append(str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

### Pull specific data from API ###

ENTITIES_TO_PULL = [
    # "countries",
    # "leagues",
    # "teams",
    # "fixtures",
    "fixtures_update",
    # "stats"
]

api_config = ApiConfig()
fixture_fetcher = FixtureFetcher(config=api_config)
teams_fetcher = TeamFetcher(config=api_config)

stats_fetcher = StatsFetcher(config=api_config)


def main() -> None:
    for entity in ENTITIES_TO_PULL:
        if entity == "teams":
            country_ids_to_pull_teams = []
            teams_fetcher.pull_teams_for_countries_list(country_ids_to_pull_teams)
        elif entity == "fixtures":
            league_ids_to_pull_fixtures = []
            for league in league_ids_to_pull_fixtures:
                fixture_fetcher.pull_single_league_fixtures_for_all_seasons(league)
                # api.pull_single_league_fixtures_for_single_season(539, 2017)
        # elif entity == "fixtures_update":
        #     fixture_fetcher.pull_finished_fixtures()
        elif entity == "fixtures/statistics":
            # TODO: ...
            data = stats_fetcher.fetch_data(entity)
            stats_fetcher.write_response_to_json(data, entity)


if __name__ == "__main__":
    main()
