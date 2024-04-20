import time
from models.countries import Country
from models.fixtures_events import FixtureEvent
from models.fixtures_players_stats import FixturePlayerStat
from models.fixtures_stats import FixtureStat
from models.leagues import League
from models.fixtures import Fixture
from models.seasons import Season
from models.teams import Team
from services.api import (
    check_subscription_status,
    get_json_from_api, pull_single_league_fixtures_for_all_seasons, pull_single_league_fixtures_for_single_season,
    pull_updated_fixtures, pull_teams_for_countries_list, write_response_to_json,
)
from services.db import (
    check_db_connection,
    drop_all_tables,
    create_all_tables,
)
from utils.utils import (
    search_overcome_pairs,
    insert_missing_teams_into_db,
    write_df_to_csv, )
from data_processing.data_parsing import parse_countries, parse_leagues, parse_teams, parse_seasons, parse_fixtures, \
    parse_events, parse_fixture_stats, parse_fixture_player_stats

### MODES ###
# RECREATE - recreate tables and populate tables with existing data
# PULL_ONLY - pull updated data from API (countries, leagues, teams, fixtures)
# FULL - pull updated data and recreate tables
# OVERCOMES - overcome games calculation, no DB operations

MODE = "PULL_ONLY"
ENTITIES_TO_PULL = [
    # "countries",
    # "leagues",
    # "teams",
    # "fixtures",
    "fixtures_update",
]


def main():
    check_db_connection()
    pull_limit = check_subscription_status()  # Doesn't work in Rapid API

    if MODE in ["PULL_ONLY", "FULL"]:
        for entity in ENTITIES_TO_PULL:
            if entity == "countries" or entity == "leagues":
                data = get_json_from_api(entity)
                write_response_to_json(data, entity)
            elif entity == "teams":
                country_ids_to_pull_teams = []
                pull_teams_for_countries_list(country_ids_to_pull_teams, pull_limit)
            elif entity == "fixtures":
                league_ids_to_pull_fixtures = []
                for league in league_ids_to_pull_fixtures:
                    pull_single_league_fixtures_for_all_seasons(league, pull_limit)
                    # pull_single_league_fixtures_for_single_season(539, 2017)
            elif entity == "fixtures_update":
                pull_updated_fixtures()
            elif entity == "fixtures/statistics":
                # TODO: ...
                data = get_json_from_api(entity)
                write_response_to_json(data, entity)


    if MODE in ["RECREATE", "FULL"]:
        #drop_all_tables()
        create_all_tables()
        #
        # start_countries = time.time()
        # ### Countries ###
        # df_countries = parse_countries()
        # Country.insert_df(df_countries)
        # end_countries = time.time()
        #
        # start_leagues = time.time()
        # ### Leagues ###
        # df_leagues = parse_leagues()
        # League.insert_df(df_leagues)
        # end_leagues = time.time()
        #
        # start_teams = time.time()
        # ### Teams ###
        # df_teams = parse_teams()
        # Team.insert_df(df_teams)
        # end_teams = time.time()
        #
        # start_seasons = time.time()
        # ### Seasons ###
        # df_seasons = parse_seasons()
        # Season.insert_df(df_seasons)
        # end_seasons = time.time()
        #
        start_fixtures = time.time()
        ### Fixtures ###
        # df_fixtures = parse_fixtures("league_seasons")
        # insert_missing_teams_into_db(df_fixtures)
        # Fixture.insert_df(df_fixtures)
        df_updated_fixtures = parse_fixtures("updates")  # TODO: add ignoring 0KB files
        Fixture.update(df_updated_fixtures)
        end_fixtures = time.time()

        start_fixture_stats = time.time()
        ### Fixture Events, Stats, Player Stats ###
        # df_fixture_events = parse_events()
        # FixtureEvent.insert_df(df_fixture_events)
        # df_fixture_stats = parse_fixture_stats()
        # FixtureStat.insert_df(df_fixture_stats)
        # df_fixture_player_stats = parse_fixture_player_stats()
        # FixturePlayerStat.insert_df(df_fixture_player_stats)
        end_fixture_stats = time.time()

        print(
            f"Execution time summary:\n"
            # f"Countries: {end_countries - start_countries:.3f} seconds\n"
            # f"Leagues: {end_leagues - start_leagues:.3f} seconds\n"
            # f"Teams: {end_teams - start_teams:.3f} seconds\n"
            # f"Seasons: {end_seasons - start_seasons:.3f} seconds\n"
            f"Fixtures: {end_fixtures - start_fixtures:.3f} seconds\n"
            f"Fixture stats: {end_fixture_stats - start_fixture_stats:.3f} seconds\n"
        )

    if MODE == "OVERCOMES":
        df_overcome_games = Fixture.get_overcome_games()
        write_df_to_csv(df_overcome_games, "overcome_games")
        print(
            f"Overcome fixtures: {len(df_overcome_games)} out of {Fixture.count_all()} all fixtures."
        )
        # search_overcome_pairs(df_overcome_games)


if __name__ == "__main__":
    main()
