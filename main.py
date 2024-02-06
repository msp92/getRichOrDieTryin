from models.countries import Country
from models.leagues import League
from models.fixtures import Fixture
from models.seasons import Season
from models.teams import Team
from services.api import check_status, pull_json_from_api, write_response_to_json
from services.db import (
    check_connection,
    drop_all_tables,
    create_all_tables,
)
from utils.utils import (
    parse_countries,
    parse_leagues,
    parse_seasons,
    parse_teams,
    pull_single_league_fixtures_for_all_seasons,
    pull_single_league_fixtures_for_single_season,
    parse_fixtures,
    search_overcome_games,
    pull_teams_for_countries_list,
    create_overcome_mask,
    insert_missing_teams_into_db,
)
import timeit


### MODES ###
# RECREATE - recreate tables and populate tables with existing data
# PULL_ONLY - pull updated data from API (countries, leagues, teams, fixtures)
# FULL - pull updated data and recreate tables
# OVERCOMES - overcome games calculation, no DB operations

MODE = "PULL_ONLY"
ENTITIES_TO_PULL = ["fixtures"]  # countries, leagues, teams, fixtures


def main():

    # Check the database connection
    check_connection()
    # Check subscription status
    check_status()

    if MODE in ["PULL_ONLY", "FULL"]:
        for entity in ENTITIES_TO_PULL:
            if entity == "countries" or entity == "leagues":
                data = pull_json_from_api(entity)
                write_response_to_json(data, entity)
            elif entity == "fixtures":
                league_ids_to_pull_fixtures = []
                for i in league_ids_to_pull_fixtures:
                    pull_single_league_fixtures_for_all_seasons(i, 100)
                    check_status()
                    # pull_single_league_fixtures_for_single_season(session, 78, 2021)
            elif entity == "teams":
                country_ids_to_pull_teams = []
                pull_teams_for_countries_list(country_ids_to_pull_teams, 100)

    if MODE in ["RECREATE", "FULL"]:
        drop_all_tables()
        create_all_tables()

        ### Countries ###
        df_countries = parse_countries()
        Country.insert_df(df_countries)

        ### Leagues ###
        df_leagues = parse_leagues()
        League.insert_df(df_leagues)

        ### Teams ###
        df_teams = parse_teams()
        Team.insert_df(df_teams)

        ### Seasons ###
        df_seasons = parse_seasons()
        Season.insert_df(df_seasons)

        ### Fixtures ###
        df_fixtures = parse_fixtures()
        insert_missing_teams_into_db(df_fixtures)
        Fixture.insert_df(df_fixtures)

    if MODE == "OVERCOMES":
        ### Overcome games ###
        df_fixtures_from_db = Fixture.get_df_from_table()
        overcome_mask = create_overcome_mask(df_fixtures_from_db)
        df_overcome_games = df_fixtures_from_db.loc[overcome_mask]
        print(
            f"Overcome fixtures: {len(df_overcome_games)} out of {len(df_fixtures_from_db)} all fixtures."
        )
        search_overcome_games(df_overcome_games)


if __name__ == "__main__":
    main()
