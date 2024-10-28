import logging
from time import sleep

from requests import Response
from sqlalchemy import and_

from config.api_config import SLEEP_TIME
from models.data.fixtures import Fixture
from services.api_fetcher import APIFetcher
from services.db import Db

db = Db()


class PlayerStatsFetcher(APIFetcher):
    def get_player_stats(self, **kwargs) -> Response | None:
        return self.fetch_data("leagues", **kwargs)

    def pull_player_statistics_for_leagues_and_seasons(
        self, league_ids_to_pull: list, season_year_to_pull: str
    ) -> None:
        with db.get_session() as session:
            try:
                fixtures = (
                    session.query(Fixture.fixture_id, Fixture.league_name)
                    .filter(
                        and_(
                            Fixture.league_id.in_(league_ids_to_pull),
                            Fixture.season_year == season_year_to_pull,
                            Fixture.status.in_(["FT", "AET", "PEN", "INT", "ABD"]),
                        )
                    )
                    .all()
                )
                for fixture in fixtures:
                    logging.info("Sleeping for a few seconds to avoid reaching limit.")
                    sleep(SLEEP_TIME)
                    try:
                        logging.info(
                            f"Pulling player statistics for fixture: {fixture.fixture_id} "
                            f"from league: {fixture.league_name}, season: {season_year_to_pull}..."
                        )
                        statistics_fixtures_data = self.fetch_data(
                            f"fixtures/players?fixture={fixture.fixture_id}"
                        )
                    except Exception as e:
                        logging.error(e)
                        continue
                    self.write_response_to_json(
                        statistics_fixtures_data,
                        f"{fixture.fixture_id}_player_statistics",
                        "player_statistics",
                    )
            except Exception as e:
                # Handle any exceptions or errors that occur during the connection test
                logging.error(f"Connection Error: {e}")
                raise Exception
