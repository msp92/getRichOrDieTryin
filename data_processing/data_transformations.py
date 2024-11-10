import csv
from typing import Dict, Any

import numpy as np
import pandas as pd

from config.vars import DATA_DIR
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))


def transform_statistics_fixtures(json_data: Dict[str, Any]) -> pd.DataFrame:
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


def transform_player_statistics(json_data: Dict[str, Any]) -> pd.DataFrame:
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


def transform_events(json_data: Dict[str, Any]) -> pd.DataFrame:
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


def create_referees_lkp_dict_from_csv(filename: str) -> dict[str, str]:
    # Initialize an empty dictionary for the mapping
    referee_mapping = {}

    # Read the CSV file and populate the dictionary
    with open(f"{DATA_DIR}/fixtures/{filename}", mode="r", encoding="utf-8") as infile:
        reader = csv.reader(infile)
        next(reader)  # Skip the header row
        for rows in reader:
            original_name, silver_name = rows
            referee_mapping[original_name] = silver_name
        return referee_mapping


def adjust_date_range_overlaps(coaches_df: pd.DataFrame) -> pd.DataFrame:
    coaches_df["start_date"] = pd.to_datetime(coaches_df["start_date"])
    coaches_df["end_date"] = pd.to_datetime(
        coaches_df["end_date"], errors="coerce"
    )  # 'coerce' will convert None to NaT

    coaches_df = coaches_df.sort_values(by="start_date").reset_index(drop=True)

    for i in range(1, len(coaches_df)):
        # Check for overlap
        if (
            pd.notna(coaches_df.loc[i, "start_date"])
            and pd.notna(coaches_df.loc[i - 1, "end_date"])
            and coaches_df.loc[i, "start_date"] <= coaches_df.loc[i - 1, "end_date"]
        ):
            # Adjust the previous end date to be the day before the current start date
            coaches_df.at[i - 1, "end_date"] = coaches_df.loc[
                i, "start_date"
            ] - pd.Timedelta(days=1)

    coaches_df.replace({np.nan: None}, inplace=True)

    # coaches_df["start_date"] = coaches_df["start_date"].dt.date
    # coaches_df["end_date"] = coaches_df["end_date"].dt.date
    return coaches_df
