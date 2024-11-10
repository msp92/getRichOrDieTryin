import logging
import os
import pandas as pd
from config.vars import DATA_DIR, ROOT_DIR
from helpers.utils import get_df_from_json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))


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
    all_dfs = []
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
                        json_data = get_df_from_json(file_name[:-5], sub_dir=sub_dir)

                        if not json_data.empty:
                            all_dfs.append(json_data)
                        else:
                            logging.warning(f"JSON data is empty for file: {file_path}")
                    except FileNotFoundError as e:
                        logging.error(f"Error loading JSON file: {str(e)}")
    except FileNotFoundError as e:
        logging.error(f"Error loading JSON file: {str(e)}")

    # FIXME: FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated.
    #  In a future version, this will no longer exclude empty or all-NA columns when determining the result dtypes.
    #  To retain the old behavior, exclude the relevant entries before the concat operation.
    # Combine all DataFrames into a single DataFrame
    combined_df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
    return combined_df
