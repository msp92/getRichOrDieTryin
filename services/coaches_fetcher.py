import csv
from time import sleep

from requests import Response

from config.vars import SLEEP_TIME
from services.api_fetcher import APIFetcher


class CoachesFetcher(APIFetcher):
    def get_coaches(self, **kwargs) -> Response | None:
        return self.fetch_data("couchs", **kwargs)

    @staticmethod
    def pull_coaches_for_all_teams(self):
        # TODO: replace with teams from table
        with open("teams_to_pull_coaches.csv", mode="r", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            for team_id in reader:
                endpoint = f"coachs?team={team_id[0]}"
                resp = self.fetch_data(endpoint)
                self.write_response_to_json(
                    resp, f"coach_team_{team_id[0]}", "coaches"
                )
                sleep(SLEEP_TIME)
