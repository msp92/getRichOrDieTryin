from requests import Response
from time import sleep
from typing import Union

from config.entity_names import COACHES_DIR, COACHES_API_ENDPOINT
from config.vars import SLEEP_TIME
from services.fetchers.api_fetcher import ApiFetcher


class CoachesFetcher(ApiFetcher):
    def get_coaches(self, **kwargs: dict[str, Union[int, str]]) -> Response | None:
        return self.fetch_data("couchs", **kwargs)

    def pull_coaches_for_all_teams(self) -> None:
        from models.data_warehouse.main import Team

        teams_df = Team.get_df_from_table()
        for team_id in teams_df["team_id"]:
            endpoint = f"{COACHES_API_ENDPOINT}?team={team_id[0]}"
            resp = self.fetch_data(endpoint)
            self.write_response_to_json(resp, f"COACH_TEAM_{team_id[0]}", COACHES_DIR)
            sleep(SLEEP_TIME)
