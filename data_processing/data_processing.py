import logging
import os
import time

import pandas as pd

from config.config import SOURCE_DIR
from utils.utils import get_df_from_json


def load_all_files_from_directory(directory_path: str):
    """
        Load all JSON files from a directory and combine them into a single DataFrame.

        Args:
            directory_path (str): The path to the directory containing JSON files.

        Returns:
            pd.DataFrame: A DataFrame containing the combined data from all JSON files in the directory.

        Raises:
            FileNotFoundError: If the specified directory does not exist.
    """
    start = time.time()
    logging.info(f"Collecting {directory_path} data...")
    all_dfs = []

    try:
        # TODO: why it list files not existing files?
        for root, _, files in os.walk(f"{SOURCE_DIR}/{directory_path}"):
            if not files:
                raise Exception(f"No files in {SOURCE_DIR}/{directory_path}")
            for file_name in files:
                if file_name.endswith(".json"):
                    file_path = os.path.join(root, file_name)
                    try:
                        json_data = get_df_from_json(file_name[:-5], sub_dir=directory_path)
                        if not json_data.empty:
                            all_dfs.append(json_data)
                        else:
                            logging.warning(f"JSON data is empty for file: {file_path}")
                    except FileNotFoundError as e:
                        print(f"Error loading JSON file: {str(e)}")
    except FileNotFoundError as e:
        logging.error(f"Error loading JSON file: {str(e)}")

    # Combine all DataFrames into a single DataFrame
    combined_df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
    end = time.time()
    logging.info(f"Loading file with os.walk: {end-start}")
    return combined_df
