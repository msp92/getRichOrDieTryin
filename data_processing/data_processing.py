import logging
import os
import pandas as pd

from pathlib import Path
from typing import List

from config.vars import DATA_DIR, ROOT_DIR
from helpers.utils import get_df_from_json


def load_all_files_from_data_directory(sub_dir: str) -> pd.DataFrame:
    """
    Load all JSON files from a directory and combine them into a single DataFrame.

    Args:
        sub_dir (str): The path to the directory containing JSON files.

    Returns:
        pd.DataFrame: A DataFrame containing the combined data from all JSON files in the directory.

    Raises:
        FileNotFoundError: If the specified directory does not exist.
    """
    logging.info(f"Collecting {DATA_DIR}/{sub_dir} data...")
    list_of_dfs = []
    try:
        for root, _, files in os.walk(f"{ROOT_DIR}/{DATA_DIR}/{sub_dir}"):
            if root != f"{ROOT_DIR}/{DATA_DIR}/{sub_dir}":
                continue  # Skip walking into the root directory
            if not files:
                raise Exception(f"No files in {DATA_DIR}/{sub_dir}")
            for file_name in files:
                if file_name.endswith(".json"):
                    file_path = os.path.join(root, file_name)
                    try:
                        df = get_df_from_json(file_name[:-5], sub_dir=sub_dir)
                        if not df.empty:
                            list_of_dfs.append(df)
                        else:
                            logging.warning(f"JSON data is empty for file: {file_path}")
                    except FileNotFoundError as e:
                        logging.error(f"Error loading JSON file: {str(e)}")
    except FileNotFoundError as e:
        logging.error(f"Error loading JSON file: {str(e)}")

    all_dfs = [df for df in list_of_dfs if list_of_dfs and not df.empty]
    combined_df = pd.concat(all_dfs, ignore_index=True)
    return combined_df


def load_all_files_paths_from_data_directory(sub_dir: str) -> List[Path]:
    """
    Collect all JSON file paths from a directory.

    Args:
        sub_dir (str): The path to the directory containing JSON files.

    Returns:
        List[Path]: A list of file paths for all JSON files in the directory.

    Raises:
        FileNotFoundError: If the specified directory does not exist.
        Exception: If no JSON files are found in the directory.
    """
    logging.info(f"Collecting JSON file paths from {DATA_DIR}/{sub_dir}...")
    file_paths = []
    target_dir = Path(f"{ROOT_DIR}/{DATA_DIR}/{sub_dir}")

    if not target_dir.exists():
        raise FileNotFoundError(f"The directory {target_dir} does not exist.")

    for file in target_dir.glob("*.json"):
        file_paths.append(file)

    if not file_paths:
        raise Exception(f"No JSON files found in {DATA_DIR}/{sub_dir}")

    return file_paths
