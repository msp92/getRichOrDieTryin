import logging
import os
import pandas as pd
from datetime import datetime
from config.vars import SOURCE_DIR
from helpers.utils import get_df_from_json


def load_all_files_from_directory(directory_path: str) -> pd.DataFrame:
    """
    Load all JSON files from a directory and combine them into a single DataFrame.

    Args:
        directory_path (str): The path to the directory containing JSON files.

    Returns:
        pd.DataFrame: A DataFrame containing the combined data from all JSON files in the directory.

    Raises:
        FileNotFoundError: If the specified directory does not exist.
    """
    logging.info(f"Collecting {directory_path} data...")
    all_dfs = []
    try:
        for root, _, files in os.walk(f"../{SOURCE_DIR}/{directory_path}"):
            if root != f"../{SOURCE_DIR}/{directory_path}":
                continue  # Skip walking into the root directory
            if not files:
                raise Exception(f"No files in {SOURCE_DIR}/{directory_path}")
            for file_name in files:
                if file_name.endswith(".json"):
                    file_path = os.path.join(root, file_name)
                    try:
                        json_data = get_df_from_json(
                            file_name[:-5], sub_dir=directory_path
                        )

                        # Assign timestamp from filename for each single fixture
                        if directory_path == "fixtures/updates":
                            timestamp_str = file_name.split("_")[2].split(".")[0]
                            json_data["update_date"] = datetime.strptime(
                                timestamp_str, "%Y%m%d%H%M%S"
                            )

                        if not json_data.empty:
                            all_dfs.append(json_data)
                        else:
                            logging.warning(f"JSON data is empty for file: {file_path}")
                    except FileNotFoundError as e:
                        print(f"Error loading JSON file: {str(e)}")
    except FileNotFoundError as e:
        logging.error(f"Error loading JSON file: {str(e)}")

    # FIXME: FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated.
    #  In a future version, this will no longer exclude empty or all-NA columns when determining the result dtypes.
    #  To retain the old behavior, exclude the relevant entries before the concat operation.
    # Combine all DataFrames into a single DataFrame
    combined_df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
    return combined_df
