import logging
from time import sleep

from config.entity_names import TEAMS_DIR, TEAMS_API_ENDPOINT
from config.vars import SLEEP_TIME
from models.data_warehouse.main import Country
from services.fetchers.api_fetcher import ApiFetcher
from services.db import Db

db = Db()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class TeamFetcher(ApiFetcher):
    def fetch_all_teams(self) -> None:
        with db.get_session() as session:
            try:
                countries = [
                    country[0] for country in session.query(Country.country_name).all()
                ]

                if countries:
                    for country_name in countries:
                        logging.info(f"Pulling teams for {country_name}...")
                        teams_data = self.fetch_data(
                            f"{TEAMS_API_ENDPOINT}?country={country_name}"
                        )
                        sleep(SLEEP_TIME)
                        # Check if teams_data is not empty
                        if not teams_data:
                            logging.info("No data found for", country_name)
                            continue
                        self.write_response_to_json(
                            teams_data, f"TEAMS_{country_name}", TEAMS_DIR
                        )
            except Exception as e:
                logging.error(e)
