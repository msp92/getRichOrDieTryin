import csv
import logging
import os
import shutil
import json
from itertools import product
import pandas as pd
from config.api_config import SOURCE_DIR
from data_processing.data_transformations import (
    transform_statistics_fixtures,
    transform_player_statistics,
    transform_events,
)
from models.data.main.countries import Country
from models.data.fixtures.fixtures import Fixture
from models.analytics.breaks.pairs import Pair
from models.data.main.teams import Team
from services.db import Db

db = Db()


def write_response_to_json(response, filename, subdir=""):
    if response:
        file_path = f"../{SOURCE_DIR}/{subdir}/{filename}.json"
        try:
            with open(file_path, "w") as file:
                json.dump(response.json(), file)
            return True
        except (AttributeError, IOError) as e:
            logging.error(f"Writing response to '{file_path}' has failed: {e}")
    return False


# TODO: replace with more general like below
# def write_to_json(data, file_path):
#     """Write data to a JSON file."""
#
#     with open(file_path, "w") as file:
#         json.dump(data, file, indent=4)


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
        with open(f"../{SOURCE_DIR}/{sub_dir}/{filename}.json", "r") as file:
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
        raise FileNotFoundError(
            f"File '{filename}.json' not found in directory '{sub_dir}'"
        )
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(
            f"Error decoding JSON data: {str(e)}", doc="doc", pos="pos"
        )


def write_df_to_csv(df, filename):
    df.to_csv(f"{SOURCE_DIR}/{filename}.csv", index=False)


def write_to_csv(data, file_path):
    # Write data to CSV file
    with open(file_path, mode="a", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(data)


def move_json_files_between_directories(source_dir, target_dir):
    # List only JSON files in the source directory
    files_to_move = [
        file for file in os.listdir(f"../{source_dir}") if file.endswith(".json")
    ]

    # Create the child directory if it doesn't exist
    if not os.path.exists(f"../{target_dir}"):
        os.makedirs(f"../{target_dir}")

    # Move each file to the child directory
    for file_name in files_to_move:
        source_file_path = os.path.join(f"../{source_dir}", file_name)
        dest_file_path = os.path.join(f"../{target_dir}", file_name)
        shutil.move(source_file_path, dest_file_path)


def search_overcome_pairs(df):
    unique_team_ids = pd.unique(pd.concat([df["home_team_id"], df["away_team_id"]]))
    unique_team_ids_df = pd.DataFrame({"team_id": unique_team_ids})
    teams_df = Team.get_df_from_table()

    unique_teams_df = pd.merge(unique_team_ids_df, teams_df, how="left", on="team_id")

    # Iterate over the unique 'team_id's
    for i, first_team_row in unique_teams_df.iterrows():
        logging.info(
            f"[FIRST TEAM] Index: {i}/{len(unique_teams_df)}, team: {first_team_row['team_name']} ({first_team_row['country_name']})"
        )
        # Filter the overcome games for the current 'team_id'
        first_team_df = df[
            (df["home_team_id"] == first_team_row["team_id"])
            | (df["away_team_id"] == first_team_row["team_id"])
        ]
        # Prepare the data for calculations: convert pandas object => datetime => UNIX timestamp
        first_team_df = first_team_df.copy()  # Avoid SettingWithCopyWarning
        first_team_df.loc[:, "date_numeric"] = (
            pd.to_datetime(first_team_df["date"]).astype("int64") // 10**9
        )  # Convert nanoseconds to seconds

        # Check if the team has at least two games
        if len(first_team_df) < 5:
            continue

        logging.info("Searching for second team...")
        # Iterate over the overcome games of the current team
        for j, second_team_row in unique_teams_df.iterrows():
            if j > i:
                second_team_df = df[
                    (df["home_team_id"] == second_team_row["team_id"])
                    | (df["away_team_id"] == second_team_row["team_id"])
                ]
                # Prepare the data for calculations: convert pandas object => datetime => UNIX timestamp
                second_team_df = second_team_df.copy()  # Avoid SettingWithCopyWarning
                second_team_df.loc[:, "date_numeric"] = (
                    pd.to_datetime(second_team_df["date"]).astype("int64") // 10**9
                )  # Convert nanoseconds to seconds

                # Check if the team has at least two overcome games
                if len(second_team_df) < 5:
                    continue

                # Prepare combinations of overcome games for both teams
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
                                        "home_team_name": unique_teams_df.query(
                                            f"team_id == {row_first['home_team_id']}"
                                        ).iloc[0]["team_name"],
                                        "away_team_id": row_first["away_team_id"],
                                        "away_team_name": unique_teams_df.query(
                                            f"team_id == {row_first['away_team_id']}"
                                        ).iloc[0]["team_name"],
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
                                        "home_team_name": unique_teams_df.query(
                                            f"team_id == {row_second['home_team_id']}"
                                        ).iloc[0]["team_name"],
                                        "away_team_id": row_second["away_team_id"],
                                        "away_team_name": unique_teams_df.query(
                                            f"team_id == {row_second['away_team_id']}"
                                        ).iloc[0]["team_name"],
                                    }
                                ]
                            ),
                        ]
                    )
                    for (_, row_first), (_, row_second) in combinations
                    if abs(row_first["date_numeric"] - row_second["date_numeric"])
                    <= 5 * 24 * 3600  # 5 days in seconds
                    and row_first["fixture_id"] != row_second["fixture_id"]
                ]

                comb_length = len(result_combinations)

                if comb_length > 5:
                    # Write pair details into csv file
                    result_df = pd.concat(result_combinations).reset_index(drop=True)
                    # result_df.insert(0, "pair_id", pair_counter)
                    # pair_counter += 1
                    result_df.to_csv(
                        f"{SOURCE_DIR}/overcome_games/"
                        f"{comb_length}_{first_team_row['team_id']}&{second_team_row['team_id']}.csv",
                        index=False,
                    )
                    logging.info(
                        f"[SAVED] {comb_length}-COMBO FOR TEAMS: {first_team_row['team_name']} & {second_team_row['team_name']}"
                    )

                    # Insert general pair information
                    pair_df = pd.DataFrame(
                        {
                            "team_id_1": first_team_row["team_id"],
                            "team_name_1": first_team_row["team_name"],
                            "team_id_2": second_team_row["team_id"],
                            "team_name_2": second_team_row["team_name"],
                            "size": comb_length,
                            "first_game_date": min(result_df["date"]),
                            "last_game_date": max(result_df["date"]),
                        },
                        index=[0],
                    )

                    Pair.upsert(pair_df)


def search_overcome_triangles():
    pass


def insert_missing_teams_into_db(df):
    logging.info("Checking for teams in fixtures that are missing in Team table...")
    # Get unique teams from all pulled fixtures
    unique_team_ids = pd.unique(pd.concat([df["home_team_id"], df["away_team_id"]]))
    unique_team_ids_df = pd.DataFrame({"team_id": unique_team_ids})

    # Mark which teams exists in Team table and which do not
    merged_df = pd.merge(
        unique_team_ids_df,
        Team.get_df_from_table(),
        on="team_id",
        how="left",
        indicator=True,
    )
    # Get only those who do not exist in Team table
    missing_ids_df = merged_df[merged_df["_merge"] == "left_only"].drop(
        columns=["_merge"]
    )
    logging.info(
        f"Number of missing teams from current fixtures: {len(missing_ids_df)}"
    )

    # Get fixtures of missing teams
    missing_teams_in_fixtures_df = df[
        df["home_team_id"].isin(missing_ids_df["team_id"])
        | df["away_team_id"].isin(missing_ids_df["team_id"])
    ]
    # Get filtered home/away dfs
    home_teams_missing = missing_teams_in_fixtures_df[
        ["home_team_id", "home_team_name", "league_id", "country_name"]
    ].rename(columns={"home_team_id": "team_id", "home_team_name": "team_name"})
    away_teams_missing = missing_teams_in_fixtures_df[
        ["away_team_id", "away_team_name", "league_id", "country_name"]
    ].rename(columns={"away_team_id": "team_id", "away_team_name": "team_name"})

    # Merge home/away dfs
    unique_teams_df = pd.concat(
        [home_teams_missing, away_teams_missing]
    ).drop_duplicates(subset=["team_id", "team_name", "country_name"])
    # Get only missing teams
    unique_teams_filtered_df = unique_teams_df[
        unique_teams_df["team_id"].isin(missing_ids_df["team_id"])
    ].sort_values(by="team_id", ascending=True)
    unique_teams_filtered_df["logo"] = ""

    # Get correct unique teams
    missing_teams_to_insert_df = unique_teams_filtered_df.drop_duplicates(
        subset="team_id", keep=False
    )
    # Get problematic duplicates
    teams_to_fix_df = unique_teams_filtered_df[
        unique_teams_filtered_df.duplicated(subset="team_id", keep=False)
    ]

    def choose_duplicates(group):
        # Check if 'country_id' values are the same (case: different team_name for the same team)
        if group["team_name"].nunique() == 2:
            # Remove row with shorter 'team_name'
            shortest_name_index = group["team_name"].str.len().idxmax()
            return group.drop(index=shortest_name_index)

        # If 'country_id' values are different (case: different country for the same team)
        else:
            # Check if one row has country_id=166 (case: World & other country)
            if "World" in group["country_name"].values:
                # Remove row with 'country_id'=166
                index_to_remove = group[group["country_name"] == "World"].index
                return group.drop(index=index_to_remove)
            else:
                # Remove row with smaller country_id (case: two different countries - Aruba(8) & Netherlands(90))
                min_country_id_index = group["country_name"].idxmin()
                return group.drop(index=min_country_id_index)

    # Apply the custom function to handle duplicates
    deduplicated_teams_df = teams_to_fix_df.groupby("team_id").apply(choose_duplicates)

    # Merge both subsets
    concatenated_df = pd.concat([missing_teams_to_insert_df, deduplicated_teams_df])
    # Add country_id from League table
    final_df = pd.merge(
        concatenated_df, Country.get_df_from_table(), on="country_name", how="left"
    ).filter(items=["team_id", "country_id", "country_name", "team_name", "logo"])
    Team.upsert(final_df)

    def choose_duplicates(group):
        # Check if 'country_id' values are the same (case: different team_name for the same team)
        if group["team_name"].nunique() == 2:
            # Remove row with shorter 'team_name'
            shortest_name_index = group["team_name"].str.len().idxmax()
            return group.drop(index=shortest_name_index)

        # If 'country_id' values are different (case: different country for the same team)
        else:
            # Check if one row has country_id=166 (case: World & other country)
            if "World" in group["country_name"].values:
                # Remove row with 'country_id'=166
                index_to_remove = group[group["country_name"] == "World"].index
                return group.drop(index=index_to_remove)
            else:
                # Remove row with smaller country_id (case: two different countries - Aruba(8) & Netherlands(90))
                min_country_id_index = group["country_name"].idxmin()
                return group.drop(index=min_country_id_index)

    # Apply the custom function to handle duplicates
    deduplicated_teams_df = teams_to_fix_df.groupby("team_id").apply(choose_duplicates)

    # Merge both subsets
    concatenated_df = pd.concat([missing_teams_to_insert_df, deduplicated_teams_df])
    # Add country_id from League table
    final_df = pd.merge(
        concatenated_df, Country.get_df_from_table(), on="country_name", how="left"
    ).filter(items=["team_id", "country_id", "country_name", "team_name", "logo"])
    Team.upsert(final_df)


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
    with db.get_session() as session:
        try:
            fixtures_df = pd.read_sql_query(
                session.query(Fixture)
                .filter(
                    (Fixture.league_id == league_id)
                    & (Fixture.season_year == season_year)
                    & (Fixture.status == "FT")
                )
                .statement,
                db.engine,
            )
        except Exception:
            raise Exception

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
        home_team = result["home_team_name"]
        away_team = result["away_team_name"]
        home_goals = int(result["goals_home"])
        away_goals = int(result["goals_away"])
        table = update_table(table, home_team, away_team, home_goals, away_goals)

    table = table.sort_values(by=["PTS", "GF"], ascending=[False, False]).reset_index(
        drop=True
    )
    table.index += 1
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
            logging.error(f"Error: {str(e)}")
            continue

    final_df = pd.concat(team_stats_df_list)
    final_df["no_draw"] = (
        final_df["form"].str[::-1].apply(lambda x: x.find("D") if "D" in x else -1)
    )
    final_df = final_df.filter(items=["team_name", "no_draw"])
    final_df = final_df[final_df["no_draw"] > 0]
    final_df.to_csv(
        f"{SOURCE_DIR}/team_stats_no_draw.csv",
        index=False,
    )
