import csv
import os
import shutil
import json
import unicodedata
from typing import List, Union

import pandas as pd
from config.vars import DATA_DIR, ROOT_DIR
from data_processing.data_transformations import (
    transform_statistics_fixtures,
    transform_player_statistics,
    transform_events,
)
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))


def get_df_from_json(filename: str, sub_dir: str = "") -> pd.DataFrame:
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
        with open(f"{ROOT_DIR}/{DATA_DIR}/{sub_dir}/{filename}.json", "r") as file:
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
            f"Error decoding JSON data: {str(e)}", doc="doc", pos=0
        )


def write_df_to_csv(df: pd.DataFrame, filename: str) -> None:
    df.to_csv(f"{ROOT_DIR}/{DATA_DIR}/{filename}.csv", index=False)


def append_data_to_csv(data: Union[str, List[str]], file_path: str) -> None:
    # Write data to CSV file
    with open(file_path, mode="a", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile, doublequote=True)
        # Convert string to a single-element list, so writer.writerow works consistently
        rows = [data] if isinstance(data, str) else data
        writer.writerow(rows)


def move_json_files_between_directories(source_dir: str, target_dir: str) -> None:
    # List only JSON files in the source directory
    files_to_move = [
        file for file in os.listdir(source_dir) if file.endswith(".json")
    ]

    # Create the child directory if it doesn't exist
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    # Move each file to the child directory
    for file_name in files_to_move:
        source_file_path = os.path.join(source_dir, file_name)
        dest_file_path = os.path.join(target_dir, file_name)
        shutil.move(source_file_path, dest_file_path)


def utf8_to_ascii(text: str) -> str:
    # Normalize to decompose special characters
    normalized = unicodedata.normalize("NFD", text)
    # Encode to ASCII, ignoring characters that can't be converted
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")

    return ascii_text
