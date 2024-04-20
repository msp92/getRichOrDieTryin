import os
import re
import json
import time
from itertools import product
import pandas as pd
from config.config import SOURCE_DIR
from data_processing.data_transformations import transform_statistics_fixtures, transform_player_statistics, \
    transform_events
from models.countries import Country
from models.fixtures import Fixture
from models.leagues import League
from models.teams import Team


def get_df_from_json(filename: str, sub_dir="") -> pd.DataFrame:
    """
        Read JSON data from a file and convert it to a pandas DataFrame.

        Args:
            filename (str): The name of the JSON file (without the ".json" extension).
            sub_dir (str, optional): The subdirectory within SOURCE_DIR where the file is located. Default is "".

        Returns:
            pd.DataFrame: A DataFrame containing the normalized JSON data.

        Raises:
            FileNotFoundError: If the specified JSON file is not found.
            JSONDecodeError: If the JSON data cannot be decoded.
    """
    try:
        with open(f"{SOURCE_DIR}/{sub_dir}/{filename}.json", "r") as file:
            json_data = json.load(file)
            df = pd.json_normalize(json_data["response"])

            if sub_dir == "statistics_fixtures":
                df = transform_statistics_fixtures(json_data)
            elif sub_dir == "player_statistics":
                df = transform_player_statistics(json_data)
            # TODO: replace back to only "events"
            elif sub_dir == "events_original":
                df = transform_events(json_data)
            return df
    except FileNotFoundError:
        raise FileNotFoundError(f"File '{filename}.json' not found in directory '{sub_dir}'")
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Error decoding JSON data: {str(e)}", doc="doc", pos="pos")


def write_df_to_csv(df, filename):
    df.to_csv(f"{SOURCE_DIR}/{filename}.csv", index=False)


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
                                    "home_team_name": unique_teams_df.query(f"team_id == {row_first['home_team_id']}").iloc[0]['team_name'],
                                    "away_team_id": row_first["away_team_id"],
                                    "away_team_name": unique_teams_df.query(f"team_id == {row_first['away_team_id']}").iloc[0]['team_name']
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
# Search for teams existing in pulled fixtures that are not yet inserted into Team table
# TODO: refactor DONE, add docstring


def insert_missing_teams_into_db(df):
    print("Checking for teams in fixtures that are missing in Team table...")
    # Get unique teams from all pulled fixtures
    unique_team_ids = pd.unique(pd.concat([df["home_team_id"], df["away_team_id"]]))
    unique_team_ids_df = pd.DataFrame({"team_id": unique_team_ids})

    # Mark which teams exists in Team table and which do not
    merged_df = pd.merge(
        unique_team_ids_df, Team.get_df_from_table(), on="team_id", how="left", indicator=True
    )
    # Get only those who do not exist in Team table
    missing_ids_df = merged_df[merged_df["_merge"] == "left_only"].drop(
        columns=["_merge"]
    )
    print(f"Number of missing teams from current fixtures: {len(missing_ids_df)}")

    # Get fixtures of missing teams
    missing_teams_in_fixtures_df = df[
        df["home_team_id"].isin(missing_ids_df["team_id"])
        | df["away_team_id"].isin(missing_ids_df["team_id"])
    ]
    # Get filtered home/away dfs
    home_teams_missing = missing_teams_in_fixtures_df[["home_team_id", "home_team_name", "league_id", "country_name"]].rename(
        columns={"home_team_id": "team_id", "home_team_name": "team_name"}
    )
    away_teams_missing = missing_teams_in_fixtures_df[["away_team_id", "away_team_name", "league_id", "country_name"]].rename(
        columns={"away_team_id": "team_id", "away_team_name": "team_name"}
    )

    # Merge home/away dfs
    unique_teams_df = (
        pd.concat([home_teams_missing, away_teams_missing])
        .drop_duplicates(subset=['team_id', 'team_name', 'country_name'])
    )
    # Get only missing teams
    unique_teams_filtered_df = (unique_teams_df[
        unique_teams_df["team_id"].isin(missing_ids_df["team_id"])
    ].sort_values(by="team_id", ascending=True))
    unique_teams_filtered_df["logo"] = ""

    # Get correct unique teams
    missing_teams_to_insert_df = unique_teams_filtered_df.drop_duplicates(subset='team_id', keep=False)
    # Get problematic duplicates
    teams_to_fix_df = unique_teams_filtered_df[unique_teams_filtered_df.duplicated(subset='team_id', keep=False)]

    def choose_duplicates(group):
        # Check if 'country_id' values are the same (case: different team_name for the same team)
        if group['team_name'].nunique() == 2:
            # Remove row with shorter 'team_name'
            shortest_name_index = group['team_name'].str.len().idxmax()
            return group.drop(index=shortest_name_index)

        # If 'country_id' values are different (case: different country for the same team)
        else:
            # Check if one row has country_id=166 (case: World & other country)
            if "World" in group['country_name'].values:
                # Remove row with 'country_id'=166
                index_to_remove = group[group['country_name'] == "World"].index
                return group.drop(index=index_to_remove)
            else:
                # Remove row with smaller country_id (case: two different countries - Aruba(8) & Netherlands(90))
                min_country_id_index = group['country_name'].idxmin()
                return group.drop(index=min_country_id_index)

    # Apply the custom function to handle duplicates
    deduplicated_teams_df = teams_to_fix_df.groupby('team_id').apply(choose_duplicates)

    # Merge both subsets
    concatenated_df = pd.concat([missing_teams_to_insert_df, deduplicated_teams_df])
    # Add country_id from League table
    final_df = (
        pd.merge(concatenated_df, Country.get_df_from_table(), on="country_name", how="left")
        .filter(items=["team_id", "country_id", "country_name", "team_name", "logo"])
    )
    Team.insert_df(final_df)
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


def calculate_no_draw_csv_for_all_teams() -> None:
    teams_df = Team.get_df_from_table()
    team_stats_df_list = []
    for idx, row in teams_df.iterrows():
        try:
            team_stats_grouped, team_stats_total = Fixture.get_season_stats_by_team(
                row["team_id"], "2023"
            )
            team_stats_total.filter(items=["team_name", "form"])
            team_stats_df_list.append(team_stats_total)
        except Exception as e:  # check what is the error and handle it
            print(f"Error: {str(e)}")
            continue

    final_df = pd.concat(team_stats_df_list)
    final_df["no_draw"] = final_df["form"].str[::-1].apply(lambda x: x.find("D") if "D" in x else -1)
    final_df = final_df.filter(items=["team_name", "no_draw"])
    final_df = final_df[final_df["no_draw"] > 0]
    final_df.to_csv(
                f"{SOURCE_DIR}/team_stats_no_draw.csv",
                index=False,
            )
