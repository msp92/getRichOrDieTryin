from models.seasons import Season
from services.api import pull_json_from_api
from services.db import check_connection, get_db_connection, drop_all_tables
from utils.utils import parse_countries, parse_leagues, parse_seasons
from models.countries import Country
from models.leagues import League
import pandas as pd


def main():
    drop_all_tables()

    session = get_db_connection()

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

    #pull_json_from_api("countries")
    #pull_json_from_api("leagues")

    ### Countries ###
    df_countries = parse_countries()
    Country.insert_df(session, df_countries)
    Country.get_all(session)

    ### Leagues ###
    df_countries_from_db = Country.get_df_from_db(session)
    df_leagues = parse_leagues()
    merged_df = pd.merge(df_leagues, df_countries_from_db, left_on="country_name", right_on="name", how="left")
    final_df = (merged_df
                .rename(columns={"id_x": "id", "id_y": "country_id", "name_x": "name"})
                .filter(items=["id", "country_id", "name", "type", "logo"]))
    League.insert_df(session, final_df)
    League.get_all(session)

    ### Seasons ###
    df_leagues_from_db = League.get_df_from_db(session)
    df_seasons = parse_seasons()
    merged_df = pd.merge(df_seasons, df_leagues_from_db, left_on="league_id", right_on="id", how="left")
    final_df = merged_df.filter(items=["league_id", "country_id", "year", "start_date", "end_date", "current", "coverage"])
    Season.insert_df(session, final_df)
    Season.get_all(session)


if __name__ == "__main__":
    main()
