from config.config import (
    SOURCE_DIR
)
import json
import pandas as pd


def get_df_from_json(item):
    with open(f'{SOURCE_DIR}/{item}.json', 'r') as file:
        json_data = json.load(file)
    df = pd.DataFrame(json_data["response"])
    return df


def parse_countries() -> pd.DataFrame:
    pass


def parse_leagues() -> pd.DataFrame:
    pass


def parse_seasons() -> pd.DataFrame:
    pass
