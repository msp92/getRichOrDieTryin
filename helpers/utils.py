import csv
import os
import shutil
import json
import pandas as pd
from config.vars import SOURCE_DIR
from data_processing.data_transformations import (
    transform_statistics_fixtures,
    transform_player_statistics,
    transform_events,
)


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


def write_df_to_csv(df, filename) -> None:
    df.to_csv(f"{SOURCE_DIR}/{filename}.csv", index=False)


def append_data_to_csv(data, file_path) -> None:
    # Write data to CSV file
    with open(file_path, mode="a", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([data])


def move_json_files_between_directories(source_dir, target_dir) -> None:
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
