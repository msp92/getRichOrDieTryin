from sqlalchemy.orm import Session

from config.config import (
    SOURCE_DIR
)
import json
import pandas as pd
from models.countries import Country
from models.leagues import League


def get_df_from_json(item):
    with open(f'{SOURCE_DIR}/{item}.json', 'r') as file:
        json_data = json.load(file)
    df = pd.json_normalize(json_data["response"])
    return df


def parse_countries() -> pd.DataFrame:
    return get_df_from_json("countries")


def parse_leagues() -> pd.DataFrame:
    df = get_df_from_json("leagues").filter(items=[
        "league.id",
        "league.name",
        "league.type",
        "league.logo",
        "country.name"])
    df.rename(columns={
        "league.id": "id",
        "league.name": "name",
        "league.type": "type",
        "league.logo": "logo",
        "country.name": "country_name"},
        inplace=True)
    return df


def parse_seasons() -> pd.DataFrame:
    df = get_df_from_json("leagues")
    # Explode the 'seasons' column to create new rows
    result_df = df.explode('seasons', ignore_index=True)
    # Concatenate the exploded 'seasons' column with the original DataFrame
    result_df = pd.concat([result_df.drop(columns='seasons'), result_df['seasons'].apply(pd.Series)], axis=1)

    final_df = result_df.filter(items=[
        "league.id",
        "year",
        "start",
        "end",
        "current",
        "coverage"])

    final_df.rename(columns={"league.id": "league_id"}, inplace=True)
    final_df['start_date'] = pd.to_datetime(final_df['start'], format='%Y-%m-%d')
    final_df['end_date'] = pd.to_datetime(final_df['end'], format='%Y-%m-%d')
    final_df.drop(columns=['start', 'end'], inplace=True)
    return final_df


def extract_seasons_from_leagues(session) -> pd.DataFrame:
    df = get_df_from_json("leagues")
    df_leagues = pd.DataFrame(df["league"])
    df_countries = pd.DataFrame(df["country"])
    df_seasons = pd.DataFrame(df["seasons"])

    # TODO: Merge below DFs
    df_leagues["id"].merge()
    df_countries["id"]
    df_seasons[0]["year"]
    df_seasons[0]["start"]
    df_seasons[0]["end"]
    df_seasons[0]["current"]
    df_seasons[0]["coverage"]


    # Dla każdego rekordu znajdź id z Country według name
    countries_id_df = session.query(Country.id, Country.name).all()
    df.set_index(df["country"]["name"], inplace=True)
    countries_id_df.set_index('name', inplace=True)
    df_new = df.join(countries_id_df, on="name", how="left")
    print(df_new.columns)

    return df_new


def parse_fixtures() -> pd.DataFrame:
    return get_df_from_json("fixtures")
