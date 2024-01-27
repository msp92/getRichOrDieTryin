import pandas as pd
from models.base import Base
from models.fixtures import Fixture
from models.seasons import Season
from services.api import (
    check_status,
    pull_json_from_api,
    write_response_to_json
)
from services.db import (
    check_connection,
    drop_all_tables,
    get_db_session,
    get_db_connection
)
from utils.utils import (
    parse_countries,
    parse_leagues,
    parse_seasons,
    pull_single_league_fixtures_for_all_seasons,
    pull_single_league_fixtures_for_single_season,
    parse_fixtures,
    search_overcome_games
)
from models.countries import Country
from models.leagues import League



def main():
    # Check subscription status
    check_status()

    engine = get_db_connection()
    session = get_db_session()

    if session:
        print("Successfully connected to the database.")
        # Check the database connection
        if check_connection(session):
            print("Database connection is active.")
        else:
            print("Database connection is not active.")
            return False
    else:
        print("Failed to connect to the database.")
        return False

    if Base.metadata.tables:
        drop_all_tables()

    Base.metadata.create_all(bind=engine)

    countries_data = pull_json_from_api("countries")
    write_response_to_json(countries_data, "countries")
    leagues_data = pull_json_from_api("leagues")
    write_response_to_json(leagues_data, "leagues")

    ### Countries ###
    df_countries = parse_countries()
    Country.insert_df(session, df_countries)
    # Country.get_all(session)

    ### Leagues ###
    df_countries_from_db = Country.get_df_from_db(session)
    df_leagues = parse_leagues()
    merged_df = pd.merge(df_leagues, df_countries_from_db, left_on="country_name", right_on="name", how="left")
    final_leagues_df = (merged_df
                .rename(columns={"name_x": "name"})
                .filter(items=["league_id", "country_id", "name", "type", "logo"]))
    League.insert_df(session, final_leagues_df)
    # League.get_all(session)

    ### Fixtures (incrementally) ###
    df_fixtures = parse_fixtures()
    # TODO: do the same merge as for seasons if some column need to be included (country_id or others)
    final_fixtures_df = df_fixtures.filter(
        items=[
            "fixture_id", "league_id", "season_id", "date", "home_team_id", "away_team_id",
            "goals_home_ht", "goals_away_ht", "goals_home", "goals_away"
        ])
    Fixture.insert_df(session, final_fixtures_df)
    # Fixture.get_all(session)

    ### Seasons ###
    df_leagues_from_db = League.get_df_from_db(session)
    print(f"Check1: {df_leagues_from_db.query('league_id==276')}")
    df_seasons = parse_seasons()
    # Merging with df_leagues_from_db to include country_id
    merged_df = pd.merge(df_seasons, df_leagues_from_db, left_on="league_id", right_on="league_id", how="left")
    final_seasons_df = merged_df.filter(
        items=[
            "season_id", "league_id", "country_id", "year", "start_date", "end_date", "current", "coverage"
        ]
    )
    Season.insert_df(session, final_seasons_df)
    # Season.get_all(session)


    ### Overcome games ###
    df_fixtures_from_db = Fixture.get_df_from_db(session)
    df_overcome_games = df_fixtures_from_db[
        (
            (
                    (df_fixtures_from_db['goals_home_ht'] > df_fixtures_from_db['goals_away_ht'])
                    &
                    (df_fixtures_from_db['goals_home'] < df_fixtures_from_db['goals_away'])
            )
            |
            (
                    (df_fixtures_from_db['goals_home_ht'] < df_fixtures_from_db['goals_away_ht'])
                    &
                    (df_fixtures_from_db['goals_home'] > df_fixtures_from_db['goals_away'])
            )
        )
    ]
    search_overcome_games(df_overcome_games)


    # 3. Prepare list with order which leagues to put first
    # 4. Pull some most important leagues
    # 5. Start creating Fixture table in database

    ### Pulled leagues ###
    # World Cup = 1
    # UEFA Champions League = 2
    # UEFA Europa League = 3
    # Euro Championship = 4
    # UEFA Nations League = 5
    # Africa Cup of Nations = 6
    # Asian Cup = 7
    # World Cup (Women) = 8
    # Copa America = 9
    # Friendlies = 10
    # CONMEBOL Sudamericana = 11
    # CAF Champions League = 12
    # CONMEBOL Libertadores = 13
    # UEFA Youth League = 14
    # FIFA Club World Cup = 15
    # CONCACAF Champions League = 16

    # ENG1 = 39
    # ENG2 = 40
    # ENG3 = 41
    # ENG4 = 42
    # FRA1 = 61
    # FRA2 = 62
    # FRA3 = 63
    # GER1 = 78
    # GER2 = 79
    # GER3 = 80
    # NED1 = 88
    # NED2 = 89
    # ITA1 = 135
    # ITA2 = 136
    # ESP1 = 140
    # ESP2 = 141
    # BEL1 = 144
    # BEL2 = 145

    # TODO: rename all files to 2023_ENG1.json or ENG1_2023.json

    # pull_single_league_fixtures_for_all_seasons(session, 12, 20)
    # pull_single_league_fixtures_for_single_season(session, 78, 2021)


if __name__ == "__main__":
    main()
