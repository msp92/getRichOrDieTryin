import os
import re
import json
import time
from time import sleep
from datetime import datetime
from itertools import product
import pandas as pd
from config.config import SOURCE_DIR
from models.countries import Country
from models.fixtures import Fixture
from models.leagues import League
from models.seasons import Season
from models.teams import Team
from services.api import pull_json_from_api, write_response_to_json, get_data_from_api
from services.db import get_db_session


def get_df_from_json(filename, sub_dir=""):
    with open(f"{SOURCE_DIR}/{sub_dir}/{filename}.json", "r") as file:
        json_data = json.load(file)
    df = pd.json_normalize(json_data["response"])
    return df


# TODO: check if performance can be improved
def load_all_files_from_directory(directory_path):
    files = os.listdir(f"{SOURCE_DIR}/{directory_path}")
    print(f"Collecting {directory_path} data...")
    all_dfs = []

    # Iterate over all files
    for file_name in files:
        if file_name.endswith(".json"):
            json_data = get_df_from_json(file_name[:-5], sub_dir=directory_path)
            if json_data.empty:
                print("JSON EMPTY!")
            all_dfs.append(json_data)
        else:
            print(f"Unsupported file type: {file_name}")

    # Combine all DataFrames into a single DataFrame
    combined_df = pd.concat(all_dfs, ignore_index=True)

    return combined_df


def parse_countries() -> pd.DataFrame:
    print(f"\n** Parsing countries data **")
    df = get_df_from_json("countries").rename(columns={"name": "country_name"})
    return df


def parse_leagues() -> pd.DataFrame:
    print(f"\n** Parsing leagues data **")
    country_df = Country.get_df_from_table()
    df = get_df_from_json("leagues").rename(
        columns={
            "league.id": "league_id",
            "league.name": "name",
            "league.type": "type",
            "league.logo": "logo",
            "country.name": "country_name",
        }
    )
    final_df = pd.merge(df, country_df, on="country_name", how="left").filter(
        items=League.get_columns_list()
    ).sort_values(by="league_id")
    return final_df


def parse_teams():
    print(f"\n** Parsing teams data **")
    country_df = Country.get_df_from_table()
    df = load_all_files_from_directory("teams")
    df.rename(
        columns={
            "team.id": "team_id",
            "team.name": "team_name",
            "team.country": "country_name",
            "team.logo": "logo",
        },
        inplace=True,
    )
    final_df = pd.merge(df, country_df, on="country_name", how="left").filter(
        items=Team.get_columns_list()
    ).sort_values(by="team_id")
    return final_df


def parse_seasons() -> pd.DataFrame:
    print(f"\n** Parsing seasons data **")
    leagues_df = get_df_from_json("leagues")
    country_df = Country.get_df_from_table()
    # Explode 'season' column and create new columns from season dict values
    final_df = (
        pd.concat(
            [
                leagues_df.drop(columns="seasons"),
                leagues_df["seasons"].explode().apply(pd.Series),
            ],
            axis=1,
        ).rename(columns={"league.id": "league_id", "country.name": "country_name"})
    ).sort_values("league_id", ascending=True)
    # Convert to date and cut timestamp
    final_df["start_date"] = pd.to_datetime(
        final_df["start"], format="%Y-%m-%d"
    ).dt.date
    final_df["end_date"] = pd.to_datetime(final_df["end"], format="%Y-%m-%d").dt.date

    # Create id's in L1_S2022 format
    final_df["season_id"] = (
        "L" + final_df["league_id"].astype(str) + "_S" + final_df["year"].astype(str)
    )

    final_df = pd.merge(final_df, country_df, on="country_name", how="left")

    # As L276_2018 appeared to be a duplicate - seasons merged manually
    # assign_suffixes_for_duplicates(final_df, "season_id")

    final_df = final_df.filter(items=Season.get_columns_list())
    return final_df


def parse_fixtures(subdir) -> pd.DataFrame:
    print(f"\n** Parsing fixtures data **")
    time1 = time.time()
    df = load_all_files_from_directory(f"fixtures/{subdir}")
    time2 = time.time()
    print(f"Loading fixtures data took {time2-time1:.3f} seconds.")
    df["season_id"] = (
        "L" + df["league.id"].astype(str) + "_S" + df["league.season"].astype(str)
    )

    df["goals.home"] = df["goals.home"].fillna(999)
    df["goals.away"] = df["goals.away"].fillna(999)
    df["score.halftime.home"] = df["score.halftime.home"].fillna(999)
    df["score.halftime.away"] = df["score.halftime.away"].fillna(999)

    final_df = df.rename(
        columns={
            "fixture.id": "fixture_id",
            "fixture.date": "date",
            "fixture.referee": "referee",
            "fixture.status.short": "status",
            "league.id": "league_id",
            "league.name": "league_name",
            "league.country": "country_name",
            "league.season": "season_year",
            "league.round": "round",
            "teams.home.id": "home_team_id",
            "teams.home.name": "home_team_name",
            "teams.away.id": "away_team_id",
            "teams.away.name": "away_team_name",
            "goals.home": "goals_home",
            "goals.away": "goals_away",
            "score.halftime.home": "goals_home_ht",
            "score.halftime.away": "goals_away_ht",
        }
    ).filter(items=Fixture.get_columns_list()).sort_values("date", ascending=True)
    return final_df


def pull_single_league_fixtures_for_all_seasons(
    league_id_to_pull: int, pull_limit: int
):
    session = get_db_session()
    league_id, league_name = session.query(League.league_id, League.name).where(
        League.league_id == league_id_to_pull
    )[0]
    season_years = (
        session.query(Season.year).where(Season.league_id == league_id_to_pull).all()
    )
    count = 0
    for season_year in season_years:
        if count < pull_limit:
            print(f"Sleeping for a few seconds to avoid reaching limit.")
            sleep(7)
            try:
                print(
                    f"Pulling fixtures for {league_name}, season: {season_year[0]}..."
                )
                season_data = pull_json_from_api(
                    f"fixtures?league={league_id}&season={season_year[0]}"
                )
                write_response_to_json(
                    season_data, f"{league_id}-{league_name}-{season_year[0]}", "fixtures/league_seasons"
                )
            except Exception as e:
                print(e)
        count += 1


def pull_single_league_fixtures_for_single_season(
    league_id_to_pull: int, season_id_to_pull: int
):
    session = get_db_session()
    league_id, league_name = session.query(League.league_id, League.name).where(
        League.league_id == league_id_to_pull
    )[0]
    try:
        print(f"Pulling fixtures for {league_name}, season: {season_id_to_pull}...")
        season_data = pull_json_from_api(
            f"fixtures?league={league_id}&season={season_id_to_pull}"
        )
        write_response_to_json(
            season_data, f"{league_id}-{league_name}-{season_id_to_pull}", "fixtures/league_seasons"
        )
    except Exception as e:
        print(e)


def pull_updated_fixtures() -> None:
    dates_to_pull = Fixture.get_dates_to_update()
    for single_date in dates_to_pull:
        endpoint = f"fixtures?date={single_date}&status=FT"
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"FT_{single_date}_{timestamp}"
        subdir = "fixtures/updates"
        resp = get_data_from_api(endpoint)
        write_response_to_json(resp, filename, subdir)
        sleep(7)


# TODO: remove pull_limit and instead implement checking the current available limit
def pull_teams_for_countries_list(country_ids_list_to_pull: list, pull_limit: int):
    session = get_db_session()
    count = 0
    for country_id in country_ids_list_to_pull:
        country_name = session.query(Country.name).where(
            Country.country_id == country_id
        )[0][0]
        if count < pull_limit:
            print(f"Sleeping for a few seconds to avoid reaching limit.")
            sleep(7)
            try:
                print(f"Pulling teams for {country_name}...")
                teams_data = pull_json_from_api(f"teams?country={country_name}")
                write_response_to_json(teams_data, f"{country_id}_{country_name}", "teams")
            except Exception as e:
                print(e)
        count += 1


# Founded duplicates in leagues.json for league_ids=[276, 379, 397, 402, 542]
# It caused also problem in fixtures so problem has been resolved manually
# def assign_suffixes_for_duplicates(df, column_name):
#     # Find the duplicated values in the column_name
#     df["is_duplicate"] = df.duplicated(subset=column_name, keep=False)
#     breakpoint()
#
#     if df["is_duplicate"].any():
#         df.reset_index(drop=True, inplace=True)
#         # Calculate cumulative count and assign incremented value as a suffix
#         df.loc[df["is_duplicate"], column_name] = (
#             df[column_name]
#             + "_"
#             + df[df["is_duplicate"]]
#             .groupby(column_name)
#             .cumcount()
#             .apply(lambda x: x + 1)
#             .astype(str)
#         )
#         df.drop(columns="is_duplicate", inplace=True)
#         print("Duplicates found and suffixes added.")
#     else:
#         print("No duplicates found.")


def search_overcome_pairs(df):
    start = time.time()
    unique_team_ids = pd.unique(pd.concat([df["home_team_id"], df["away_team_id"]]))
    unique_team_ids_df = pd.DataFrame({"team_id": unique_team_ids})
    teams_df = Team.get_df_from_table()
    # TODO: merge league_name to input df
    leagues_df = League.get_df_from_table()

    unique_teams_df = pd.merge(unique_team_ids_df, teams_df, how="left", on="team_id")

    # Iterate over the unique 'team_id's
    for i, first_team_row in unique_teams_df.iterrows():
        print(
            f"[FIRST TEAM] Index: {i}/{len(unique_teams_df)}, team: {first_team_row['team_id']}-{first_team_row['team_name']}"
        )
        # Filter the overcome games for the current 'team_id'
        first_team_df = df[
            (df["home_team_id"] == first_team_row["team_id"])
            | (df["away_team_id"] == first_team_row["team_id"])
        ]
        # Check if the team has at least two games
        if len(first_team_df) < 2:
            continue

        print(f"Searching for second team...")
        # Iterate over the overcome games of the current team
        for j, second_team_row in unique_teams_df.iterrows():
            if i == j:
                continue
            second_team_df = df[
                (df["home_team_id"] == second_team_row["team_id"])
                | (df["away_team_id"] == second_team_row["team_id"])
            ]
            # Check if the team has at least two overcome games
            if len(second_team_df) < 2:
                continue

            # Get common overcome games for both teams
            combinations = list(
                product(first_team_df.iterrows(), second_team_df.iterrows())
            )
            # Filter common overcome games for those within 5 days
            result_combinations = [
                pd.concat(
                    [
                        pd.DataFrame(
                            [
                                {
                                    "fixture_id": row_first["fixture_id"],
                                    "league_name": row_first["league_name"],
                                    "round": row_first["round"],
                                    "date": row_first["date"],
                                    "referee": row_first["referee"],
                                    "home_team_id": row_first["home_team_id"],
                                    "home_team_name": unique_teams_df.query(f'team_id == {row_first['home_team_id']}').iloc[0]['team_name'],
                                    "away_team_id": row_first["away_team_id"],
                                    "away_team_name": unique_teams_df.query(f'team_id == {row_first['away_team_id']}').iloc[0]['team_name']
                                }
                            ]
                        ),
                        pd.DataFrame(
                            [
                                {
                                    "fixture_id": row_second["fixture_id"],
                                    "league_name": row_second["league_name"],
                                    "round": row_second["round"],
                                    "date": row_second["date"],
                                    "referee": row_second["referee"],
                                    "home_team_id": row_second["home_team_id"],
                                    "home_team_name": unique_teams_df.query(f"team_id == {row_second['home_team_id']}").iloc[0]['team_name'],
                                    "away_team_id": row_second["away_team_id"],
                                    "away_team_name": unique_teams_df.query(f"team_id == {row_second['away_team_id']}").iloc[0]['team_name']
                                }
                            ]
                        ),
                    ]
                )
                for (_, row_first), (_, row_second) in combinations
                if abs(
                    (
                        pd.to_datetime(row_first["date"])
                        - pd.to_datetime(row_second["date"])
                    ).days
                )
                <= 5
                and row_first["fixture_id"] != row_second["fixture_id"]
            ]
            comb_length = len(result_combinations)
            if comb_length > 1:
                result_df = pd.concat(result_combinations).reset_index(drop=True)
                result_df.to_csv(
                    f"{SOURCE_DIR}/overcome-games/"
                    f"{comb_length}-{first_team_row['team_id']}&{second_team_row['team_id']}.csv",
                    index=False,
                )
                print(
                    f"[SAVED] {comb_length}-COMBO FOR TEAMS: {first_team_row['team_name']} & {second_team_row['team_name']}"
                )
        if i == 5:
            break
    end = time.time()
    print(f"Searching overcome games for 5 teams took {end-start:.3f} seconds")


def search_overcome_triangles():
    pass


# Search for teams appearing in newly pulled fixtures that are not existing in Team table
# TODO: Optimize & refactor
def insert_missing_teams_into_db(df):
    start = time.time()
    print("Checking for missing teams from fixtures data...")
    unique_team_ids = pd.unique(pd.concat([df["home_team_id"], df["away_team_id"]]))
    unique_team_ids_df = pd.DataFrame({"team_id": unique_team_ids})
    df_teams_from_db = Team.get_df_from_table()

    # Find teams that doesn't exist in Fixture data
    merged_df = pd.merge(
        unique_team_ids_df, df_teams_from_db, on="team_id", how="left", indicator=True
    )
    missing_ids_df = merged_df[merged_df["_merge"] == "left_only"].drop(
        columns=["_merge"]
    )
    print(f"Number of missing teams from current fixtures: {len(missing_ids_df)}")

    # Check if team_id from fixtures_df is in missing_ids_df
    filtered_fixtures_df = df[
        df["home_team_id"].isin(missing_ids_df["team_id"])
        | df["away_team_id"].isin(missing_ids_df["team_id"])
    ]
    df1 = filtered_fixtures_df[["home_team_id", "home_team_name", "league_id"]].rename(
        columns={"home_team_id": "team_id", "home_team_name": "name"}
    )
    df2 = filtered_fixtures_df[["away_team_id", "away_team_name", "league_id"]].rename(
        columns={"away_team_id": "team_id", "away_team_name": "name"}
    )
    unique_teams_df = pd.concat([df1, df2]).drop_duplicates()
    unique_teams_filtered_df = unique_teams_df[
        unique_teams_df["team_id"].isin(missing_ids_df["team_id"])
    ]

    leagues_df = League.get_df_from_table()
    final_df = (
        pd.merge(unique_teams_filtered_df, leagues_df, on="league_id", how="left", indicator=True)
        .rename(columns={"name_x": "team_name"})
        .filter(items=["team_id", "country_id", "country_name", "team_name"])
    ).drop_duplicates()
    final_df["logo"] = ""

    # TODO: few teams has two countries: World & France/Cameroon
    #  Monitor how big is the problem when having all data
    final_df = final_df[(final_df['team_id'] != 8002) | (final_df['country_id'] != 166)]
    final_df = final_df[(final_df['team_id'] != 8143) | (final_df['country_id'] != 166)]
    final_df = final_df[(final_df['team_id'] != 15566) | (final_df['country_id'] != 166)]
    Team.insert_df(final_df)
    end = time.time()
    print(f"Insert missing teams took {end-start:.3f} seconds")


# Update table with single result
def update_table(table, home_team, away_team, home_goals, away_goals):
    for team, goals, opponent_goals in [
        (home_team, home_goals, away_goals),
        (away_team, away_goals, home_goals),
    ]:

        if team not in table["Team"].values:
            table = pd.concat(
                [
                    table,
                    pd.DataFrame(
                        [
                            {
                                "Team": team,
                                "G": 0,
                                "W": 0,
                                "D": 0,
                                "L": 0,
                                "GF": 0,
                                "GA": 0,
                                "PTS": 0,
                            }
                        ]
                    ),
                ]
            )

        table.loc[table["Team"] == team, "G"] += 1
        table.loc[table["Team"] == team, "GF"] += goals
        table.loc[table["Team"] == team, "GA"] += opponent_goals

        if goals > opponent_goals:
            table.loc[table["Team"] == team, "PTS"] += 3
            table.loc[table["Team"] == team, "W"] += 1
        elif goals == opponent_goals:
            table.loc[table["Team"] == team, "PTS"] += 1
            table.loc[table["Team"] == team, "D"] += 1
        else:
            table.loc[table["Team"] == team, "L"] += 1
    return table


# Calculate table for input df
def calculate_table(league_id, season_year, rounds="all_finished"):
    """
    Create full table or as of round to calculate custom power factor
    """

    def find_matching_file():
        regex = rf"{league_id}-([^-\d]+)-{season_year}"
        pattern = re.compile(regex)
        with os.scandir(f"{SOURCE_DIR}/fixtures") as files:
            for file in files:
                match = pattern.match(file.name)
                if match:
                    return file.name
            raise Exception(
                f"Could not find file for league: {league_id} & season: {season_year}"
            )

    matching_file = find_matching_file()
    fixtures_df = get_df_from_json(matching_file[:-5], "fixtures")
    filtered_fixtures_df = Fixture.filter_fixtures_by_rounds(fixtures_df, rounds)

    table = pd.DataFrame(
        columns=[
            "Team",
            "G",
            "W",
            "D",
            "L",
            "GF",
            "GA",
            "PTS",
        ]
    )

    for idx, result in filtered_fixtures_df.iterrows():
        home_team = result["teams.home.name"]
        away_team = result["teams.away.name"]
        home_goals = int(result["goals.home"])
        away_goals = int(result["goals.away"])
        table = update_table(table, home_team, away_team, home_goals, away_goals)

    table = table.sort_values(by=["PTS", "GF"], ascending=[False, False]).reset_index(
        drop=True
    )
    return table
