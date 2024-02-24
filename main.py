import time
from models.countries import Country
from models.leagues import League
from models.fixtures import Fixture
from models.seasons import Season
from models.teams import Team
from services.api import (
    check_subscription_status,
    pull_json_from_api,
    write_response_to_json,
)
from services.db import (
    check_db_connection,
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
    search_overcome_pairs,
    pull_teams_for_countries_list,
    insert_missing_teams_into_db,
    pull_updated_fixtures,
)


### MODES ###
# RECREATE - recreate tables and populate tables with existing data
# PULL_ONLY - pull updated data from API (countries, leagues, teams, fixtures)
# FULL - pull updated data and recreate tables
# OVERCOMES - overcome games calculation, no DB operations

MODE = "RECREATE"
ENTITIES_TO_PULL = [
    # "countries",
    # "leagues",
    # "teams",
    "fixtures",
    # "fixtures_update",
]


def main():
    check_db_connection()
    check_subscription_status()

    if MODE in ["PULL_ONLY", "FULL"]:
        for entity in ENTITIES_TO_PULL:
            if entity == "countries" or entity == "leagues":
                data = pull_json_from_api(entity)
                write_response_to_json(data, entity)
            elif entity == "fixtures":
                league_ids_to_pull_fixtures = []
                for i in league_ids_to_pull_fixtures:
                    pull_single_league_fixtures_for_all_seasons(i, 100)
                    check_subscription_status()
                pull_single_league_fixtures_for_single_season(311, 2020)
                pull_single_league_fixtures_for_single_season(311, 2021)
            elif entity == "fixtures_update":
                pull_updated_fixtures()
            elif entity == "teams":
                country_ids_to_pull_teams = []
                pull_teams_for_countries_list(country_ids_to_pull_teams, 100)

    if MODE in ["RECREATE", "FULL"]:
        drop_all_tables()
        create_all_tables()

        start_countries = time.time()
        ### Countries ###
        df_countries = parse_countries()
        Country.insert_df(df_countries)
        end_countries = time.time()

        start_leagues = time.time()
        ### Leagues ###
        df_leagues = parse_leagues()
        League.insert_df(df_leagues)
        end_leagues = time.time()

        start_teams = time.time()
        ### Teams ###
        df_teams = parse_teams()
        Team.insert_df(df_teams)
        end_teams = time.time()

        start_seasons = time.time()
        ### Seasons ###
        df_seasons = parse_seasons()
        Season.insert_df(df_seasons)
        end_seasons = time.time()

        start_fixtures = time.time()
        ### Fixtures ###
        df_fixtures = parse_fixtures("league_seasons")
        insert_missing_teams_into_db(df_fixtures)
        Fixture.insert_df(df_fixtures)
        df_updated_fixtures = parse_fixtures("updates")
        Fixture.update(df_updated_fixtures)
        end_fixtures = time.time()

        print(
            f"Execution time summary:\n"
            f"Countries: {end_countries - start_countries:.3f} seconds\n"
            f"Leagues: {end_leagues - start_leagues:.3f} seconds\n"
            f"Teams: {end_teams - start_teams:.3f} seconds\n"
            f"Seasons: {end_seasons - start_seasons:.3f} seconds\n"
            f"Fixtures: {end_fixtures - start_fixtures:.3f} seconds"
        )

    if MODE == "OVERCOMES":
        df_overcome_games = Fixture.get_overcome_games()
        print(
            f"Overcome fixtures: {len(df_overcome_games)} out of {Fixture.count_all()} all fixtures."
        )
        search_overcome_pairs(df_overcome_games)


if __name__ == "__main__":
    main()
