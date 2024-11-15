import csv

import numpy as np
import pandas as pd

from config.vars import DATA_DIR
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))


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
