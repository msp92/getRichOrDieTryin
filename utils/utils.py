import os
from time import sleep
from itertools import product
import json
import pandas as pd
from sqlalchemy.orm import Session
from config.config import (
    SOURCE_DIR
)
from models.leagues import League
from models.seasons import Season
from services.api import pull_json_from_api, write_response_to_json


def get_df_from_json(filename, sub_dir=""):
    with open(f'{SOURCE_DIR}/{sub_dir}/{filename}.json', 'r') as file:
        json_data = json.load(file)
    df = pd.json_normalize(json_data["response"])
    return df


def load_all_files_from_directory(directory_path):
    all_data = []
    all_data_frames = []

    # List all files in the directory
    files = os.listdir(f'{SOURCE_DIR}/{directory_path}')
    print(files)
    # Iterate over all files
    for file_name in files:
        # Check if the file is a JSON file
        if file_name.endswith('.json'):
            # Load JSON data and convert it to a DataFrame
            json_data = get_df_from_json(file_name[:-5], sub_dir=directory_path)  # Remove '.json' extension
            if json_data.empty:
                print("JSON EMPTY!")
            all_data.append(json_data.to_dict(orient='records'))
            all_data_frames.append(json_data)
        else:
            print(f"Unsupported file type: {file_name}")

    # Combine all DataFrames into a single DataFrame
    combined_df = pd.concat(all_data_frames, ignore_index=True)

    return combined_df


def parse_countries() -> pd.DataFrame:
    return get_df_from_json("countries")


def parse_leagues() -> pd.DataFrame:
    df = get_df_from_json("leagues").filter(items=[
        "league.id",
        "league.name",
        "league.type",
        "league.logo",
        "country.name"])
    df.rename(columns={
        "league.id": "league_id",
        "league.name": "name",
        "league.type": "type",
        "league.logo": "logo",
        "country.name": "country_name"},
        inplace=True)
    return df


def parse_seasons() -> pd.DataFrame:
    df = get_df_from_json("leagues")
    # Explode 'season' column and create new columns from season dict values
    final_df = pd.concat([
        df.drop(columns='seasons'),
        df['seasons'].explode().apply(pd.Series)
    ], axis=1).sort_values("league.id", ascending=True)

    # Convert to date and cut timestamp
    final_df['start_date'] = pd.to_datetime(final_df['start'], format='%Y-%m-%d').dt.date
    final_df['end_date'] = pd.to_datetime(final_df['end'], format='%Y-%m-%d').dt.date

    final_df['season_id'] = (
            "L" + final_df['league.id'].astype(str) +
            "_S" + final_df['year'].astype(str)
    )  # L1_S2022

    # As L276_2018 appeared to be a duplicate let's check whole column
    assign_suffixes_for_duplicates(final_df, "season_id")

    final_df.rename(columns={"league.id": "league_id"}, inplace=True)
    final_df = final_df[["season_id", "league_id", "year", "start_date", "end_date", "current", "coverage"]]
    return final_df


def parse_fixtures() -> pd.DataFrame:
    df = load_all_files_from_directory("fixtures")
    df['season_id'] = (
            "L" + df['league.id'].astype(str) +
            "_S" + df['league.season'].astype(str)
    )
    df["goals.home"] = df["goals.home"].fillna(100).astype(int)
    df["goals.away"] = df["goals.away"].fillna(100).astype(int)
    df["score.halftime.home"] = df["score.halftime.home"].fillna(100).astype(int)
    df["score.halftime.away"] = df["score.halftime.away"].fillna(100).astype(int)
    df.filter(items=[
        "fixture.id",
        "league.id",
        "season_id",
        "fixture.date",
        "teams.home.id",
        "teams.away.id",
        "goals.home",
        "goals.away",
        "score.halftime.home",
        "score.halftime.away"
    ])
    df.rename(columns={
        "fixture.id": "fixture_id",
        "league.id": "league_id",
        "fixture.date": "date",
        "teams.home.id": "home_team_id",
        "teams.away.id": "away_team_id",
        "goals.home": "goals_home",
        "goals.away": "goals_away",
        "score.halftime.home": "goals_home_ht",
        "score.halftime.away": "goals_away_ht"
    },
        inplace=True)
    return df


def pull_single_league_fixtures_for_all_seasons(session: Session, league_id_to_pull: int, pull_limit: int):
    league_id, league_name = session.query(League.league_id, League.name).where(League.league_id == league_id_to_pull)[0]
    count = 0
    # TODO: handle 10 requests limit per minute ???
    if count < pull_limit:
        season_years = session.query(Season.year).where(Season.league_id == league_id_to_pull).all()
        for season_year in season_years:
            if count < pull_limit:
                print(f"Sleeping for 10 seconds to avoid reaching limit.")
                sleep(10)
                try:
                    print(f"Pulling fixtures for {league_name}, season: {season_year[0]}...")
                    season_data = pull_json_from_api(f"fixtures?league={league_id}&season={season_year[0]}")
                    write_response_to_json(season_data, f"{league_id}-{league_name}-{season_year[0]}")
                except Exception as e:
                    print(e)
                count += 1


def pull_single_league_fixtures_for_single_season(session: Session, league_id_to_pull: int, season_id_to_pull: int):
    league_id, league_name = session.query(League.league_id, League.name).where(League.league_id == league_id_to_pull)[0]
    try:
        print(f"Pulling fixtures for {league_name}, season: {season_id_to_pull}...")
        season_data = pull_json_from_api(f"fixtures?league={league_id}&season={season_id_to_pull}")
        write_response_to_json(season_data, f"{league_id}-{league_name}-{season_id_to_pull}")
    except Exception as e:
        print(e)


# Founded duplicates in leagues.json for league_ids=[276, 379, 397, 402, 542]
# TODO: To check in related fixtures if causes a problem
def assign_suffixes_for_duplicates(df, column_name):
    # Find the duplicated values in the 'season_id' column
    df['is_duplicate'] = df.duplicated(subset=column_name, keep=False)

    if df['is_duplicate'].any():
        df.reset_index(drop=True, inplace=True)
        # Calculate cumulative count and assign incremented value as a suffix
        df.loc[df['is_duplicate'], column_name] = df[column_name] + '_' + df[df['is_duplicate']].groupby(column_name).cumcount().apply(lambda x: x + 1).astype(str)
        df.drop(columns='is_duplicate', inplace=True)
        print("Duplicates found and suffixes added.")
        breakpoint()
    else:
        print("No duplicates found.")


def search_overcome_games(df):
    # Initialize an empty list to store the pairs
    pairs = []
    unique_teams = pd.unique(pd.concat([df['home_team_id'], df['away_team_id']]))
    unique_teams_df = pd.DataFrame({'team_id': unique_teams})

    # Iterate over the unique 'team_id's
    for i, first_team_row in unique_teams_df.iterrows():
        print(f"[FIRST TEAM] Index: {i}/{len(unique_teams_df)}, team: {first_team_row['team_id']}")
        # Filter the dataframe for the current 'team_id'
        first_team_df = df[
            (df['home_team_id'] == first_team_row['team_id']) | (df['away_team_id'] == first_team_row['team_id'])]
        # Check if the team has at least two games
        if len(first_team_df) < 2:
            continue

        print(f"Searching for second team...")
        # Iterate over the overcome games of the current team
        for j, second_team_row in unique_teams_df.iterrows():
            if i == j:
                continue
            # print(f"[SECOND TEAM] Index: {j}/{len(unique_teams_df)}, team: {second_team_row['team_id']}")
            second_team_df = df[
                (df['home_team_id'] == second_team_row['team_id']) | (df['away_team_id'] == second_team_row['team_id'])]
            # Check if the team has at least two overcome games
            if len(second_team_df) < 2:
                continue

            # Get common overcome games for both teams
            combinations = list(product(first_team_df.iterrows(), second_team_df.iterrows()))
            # Filter common overcome games for those within 5 days
            result_combinations = [
                pd.concat([
                    pd.DataFrame([
                        {
                            "fixture_id": row_first['fixture_id'],
                            "home_team_id": row_first['home_team_id'],
                            "away_team_id": row_first['away_team_id'],
                            "date": row_first['date']
                        }
                    ]),
                    pd.DataFrame([
                        {
                            "fixture_id": row_second['fixture_id'],
                            "home_team_id": row_second['home_team_id'],
                            "away_team_id": row_second['away_team_id'],
                            "date": row_second['date']
                        }
                    ])
                ])
                for (_, row_first), (_, row_second) in combinations
                if
                abs((pd.to_datetime(row_first['date']) - pd.to_datetime(row_second['date'])).days) <= 5 and row_first[
                    'fixture_id'] != row_second['fixture_id']
            ]
            if len(result_combinations) > 1:
                result_df = pd.concat(result_combinations).reset_index(drop=True)
                result_df.to_csv(f"{SOURCE_DIR}/overcome-games/{first_team_row['team_id']}_{second_team_row['team_id']}.csv")
                print(f"[SAVED] {len(result_combinations)}-COMBO FOR TEAMS: {first_team_row['team_id']} & {second_team_row['team_id']}")
