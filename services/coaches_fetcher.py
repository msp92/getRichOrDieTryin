from time import sleep
from typing import Any

from requests import Response

from config.entity_names import COACHES_DIR, COACHES_API_ENDPOINT
from config.vars import SLEEP_TIME
from models.data.main import Team
from services.api_fetcher import APIFetcher


class CoachesFetcher(APIFetcher):
    def get_coaches(self, **kwargs: dict[str, Any]) -> Response | None:
        return self.fetch_data("couchs", **kwargs)

    def pull_coaches_for_all_teams(self) -> None:
        teams_df = Team.get_df_from_table()
        for team_id in teams_df["team_id"]:
            endpoint = f"{COACHES_API_ENDPOINT}?team={team_id[0]}"
            resp = self.fetch_data(endpoint)
            self.write_response_to_json(resp, f"COACH_TEAM_{team_id[0]}", COACHES_DIR)
            sleep(SLEEP_TIME)
