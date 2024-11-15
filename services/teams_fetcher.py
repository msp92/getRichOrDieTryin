import logging
from time import sleep
from typing import Any

from requests import Response

from config.vars import SLEEP_TIME
from models.data.main import Country
from services.api_fetcher import APIFetcher
from services.db import Db

db = Db()


class TeamFetcher(APIFetcher):
    def get_teams(self, **kwargs: dict[str, Any]) -> Response | None:
        return self.fetch_data("teams", **kwargs)

    def pull_teams_for_countries_list(
        self, country_ids_list_to_pull: list[int]
    ) -> None:
        with db.get_session() as session:
            for country_id in country_ids_list_to_pull:
                country_name = (
                    session.query(Country.name)
                    .filter(Country.country_id == country_id)
                    .scalar()
                )

                logging.info("Sleeping for a few seconds to avoid reaching limit.")
                sleep(SLEEP_TIME)

                try:
                    logging.info(f"Pulling teams for {country_name}...")
                    teams_data = self.fetch_data(f"teams?country={country_name}")
                    # Check if teams_data is not empty
                    if not teams_data:
                        logging.info("No data found for", country_name)
                        continue
                    self.write_response_to_json(
                        teams_data, f"{country_id}_{country_name}", "teams"
                    )
                except Exception as e:
                    logging.error(e)
                    continue
