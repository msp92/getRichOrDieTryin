import csv
import os
import shutil
import json
import unicodedata
from typing import List, Union

import pandas as pd

from config.entity_names import (
    FIXTURE_STATS_DIR,
    FIXTURE_EVENTS_DIR,
    FIXTURE_PLAYER_STATS_DIR,
)
from config.vars import DATA_DIR, ROOT_DIR


def get_df_from_json(filename: str, sub_dir: str) -> pd.DataFrame:
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
            # Take 'fixture_id' from response parameters
            if sub_dir in [
                FIXTURE_STATS_DIR,
                FIXTURE_PLAYER_STATS_DIR,
                FIXTURE_EVENTS_DIR,
            ]:
                df.insert(0, "fixture_id", json_data["parameters"]["fixture"])
                if sub_dir == FIXTURE_EVENTS_DIR:
                    df.insert(1, "event_id", range(1, len(df) + 1))
                else:
                    df.insert(1, "side", ["home", "away"])
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
    files_to_move = [file for file in os.listdir(source_dir) if file.endswith(".json")]

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


def safe_str_to_int_cast(value: str) -> int | None:
    try:
        return int(value) if value is not None else None
    except ValueError as e:
        raise e
