import csv

import pandas as pd

from config.api_config import SOURCE_DIR


def transform_statistics_fixtures(json_data):
    df = pd.json_normalize(json_data["response"])
    # Replace None and NaN values with a placeholder value to avoid None/NaN values in
    df.fillna("", inplace=True)
    # Get fixture_id from parameters
    df["fixture_id"] = json_data["parameters"]["fixture"]
    # Transform statistics to key:value pairs
    # TODO: make lambda more understandable
    df["statistics"] = df["statistics"].apply(
        lambda x: {d["type"]: d["value"] for d in x}
    )
    df = (
        df.groupby("fixture_id")["statistics"]
        .apply(lambda x: pd.Series(x))
        .unstack()
        .reset_index()
    )
    # Rename columns
    df.columns = ["fixture_id", "home_team_stats", "away_team_stats"]
    # Merge with original DataFrame on fixture column
    result_df = df[["fixture_id"]].merge(df, on="fixture_id")
    return result_df


def transform_player_statistics(json_data):
    df = pd.json_normalize(json_data["response"])
    # Replace None and NaN values with a placeholder value to avoid None/NaN values in
    df.fillna("", inplace=True)
    # Get fixture_id from parameters
    df["fixture_id"] = json_data["parameters"]["fixture"]
    # Transform statistics to key:value pairs
    # TODO: make lambda more understandable
    df["players"] = df.apply(
        lambda row: [
            {
                "player_id": player["player"]["id"],
                "player_name": player["player"]["name"],
                **{
                    stat: value
                    for stats in player["statistics"]
                    for stat, value in stats.items()
                },
            }
            for player in row["players"]
        ],
        axis=1,
    )
    # Group home & away stats into array
    df = (
        df.groupby("fixture_id")["players"]
        .apply(lambda x: pd.Series(x))
        .unstack()
        .reset_index()
    )
    # Rename columns
    df.columns = ["fixture_id", "home_team_players_stats", "away_team_players_stats"]
    # Merge with original DataFrame on fixture column
    result_df = df[["fixture_id"]].merge(df, on="fixture_id")
    return result_df


def transform_events(json_data):
    df = pd.json_normalize(json_data["response"])
    # FIXME: FutureWarning: Setting an item of incompatible dtype is deprecated and will raise in a future error of
    #  pandas. Value '' has dtype incompatible with float64, please explicitly cast to a compatible dtype first.
    #  df.fillna("", inplace=True)
    # Replace None and NaN values with a placeholder value to avoid None/NaN values in
    df.fillna("", inplace=True)
    # Get fixture_id from parameters
    df["fixture_id"] = json_data["parameters"]["fixture"]
    df = (
        df.groupby("fixture_id")
        .apply(lambda x: x.to_dict(orient="records"))
        .reset_index(name="events")
    )
    return df


def create_referees_lkp_dict_from_csv(filename):
    # Initialize an empty dictionary for the mapping
    referee_mapping = {}

    # Read the CSV file and populate the dictionary
    with open(
        f"../{SOURCE_DIR}/fixtures/{filename}", mode="r", encoding="utf-8"
    ) as infile:
        reader = csv.reader(infile)
        next(reader)  # Skip the header row
        for rows in reader:
            original_name, silver_name = rows
            referee_mapping[original_name] = silver_name
        return referee_mapping
