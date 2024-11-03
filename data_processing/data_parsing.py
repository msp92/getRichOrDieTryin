import logging
import sys
from pathlib import Path

import datetime as dt
import numpy as np
import pandas as pd

from data_processing.data_processing import load_all_files_from_directory
from models.data.main.coaches import Coach
from models.data.main.countries import Country
from models.data.fixtures.fixtures import Fixture
from models.data.main.leagues import League
from models.data.main.referees import Referee
from models.data.main.seasons import Season
from models.data.main.teams import Team
from helpers.utils import get_df_from_json, utf8_to_ascii

sys.path.append(str(Path(__file__).resolve().parent.parent))


def parse_countries() -> pd.DataFrame:
    logging.info("** Parsing countries data **")
    df = get_df_from_json("countries").rename(columns={"name": "country_name"})
    return df


def parse_leagues() -> pd.DataFrame:
    logging.info("** Parsing leagues data **")
    country_df = Country.get_df_from_table()
    df = get_df_from_json("leagues").rename(
        columns={
            "league.id": "league_id",
            "league.name": "league_name",
            "league.type": "type",
            "league.logo": "logo",
            "country.name": "country_name",
        }
    )
    logging.debug(League.__table__.columns)
    final_df = (
        pd.merge(df, country_df, on="country_name", how="left")
        .filter(items=[column.name for column in League.__table__.columns])
        .sort_values(by="league_id")
    )
    return final_df


def parse_teams() -> pd.DataFrame:
    logging.info("** Parsing teams data **")
    country_df = Country.get_df_from_table()
    df = load_all_files_from_directory("teams")
    df.rename(
        columns={
            "team.id": "team_id",
            "team.name": "team_name",
            "team.country": "country_name",
            "team.logo": "logo",
        },
        inplace=True,
    )
    final_df = (
        pd.merge(df, country_df, on="country_name", how="left")
        .filter(items=[column.name for column in Team.__table__.columns])
        .sort_values(by="team_id")
    )
    return final_df


def parse_seasons() -> pd.DataFrame:
    logging.info("** Parsing seasons data **")
    leagues_df = get_df_from_json("leagues")
    country_df = Country.get_df_from_table()
    # Explode 'season' column and create new columns from season dict values
    seasons_raw_df = (
        pd.concat(
            [
                leagues_df.drop(columns="seasons"),
                leagues_df["seasons"].explode().apply(pd.Series),
            ],
            axis=1,
        ).rename(columns={"league.id": "league_id", "country.name": "country_name"})
    ).sort_values("league_id", ascending=True)
    # Convert to date and cut timestamp
    seasons_raw_df["start_date"] = pd.to_datetime(
        seasons_raw_df["start"], format="%Y-%m-%d"
    ).dt.date
    seasons_raw_df["end_date"] = pd.to_datetime(
        seasons_raw_df["end"], format="%Y-%m-%d"
    ).dt.date

    # Create id's as "{league_id}_{year}_{start_date}"
    seasons_raw_df["season_id"] = (
        seasons_raw_df["league_id"].astype(str)
        + "_"
        + seasons_raw_df["year"].astype(str)
        + "_"
        + seasons_raw_df["start_date"].apply(lambda x: x.toordinal()).astype(str)
    )

    final_df = pd.merge(seasons_raw_df, country_df, on="country_name", how="left")
    problematic_rows = final_df[
        ~final_df["country_id"].apply(lambda x: pd.notnull(x) and np.isfinite(x))
    ]
    if not problematic_rows.empty:
        logging.error(
            "There are some problematic rows (probably nan) in final_df['country_id']"
        )
    final_df["country_id"] = final_df["country_id"].astype(int)

    final_df = final_df.filter(
        items=[column.name for column in Season.__table__.columns]
    )
    return final_df


def parse_fixtures(subdir: str) -> pd.DataFrame:
    """
    Parse fixtures data from JSON files in a specified subdirectory and return a DataFrame.

    Args:
        subdir (str): The subdirectory containing JSON files with fixtures data.

    Returns:
        pd.DataFrame: A DataFrame containing parsed fixtures data.

    """
    logging.info("** Parsing fixtures data **")
    df_fixtures = load_all_files_from_directory(f"fixtures/{subdir}")

    # Extract season stage and round from raw data in league.round
    df_fixtures[["season_stage", "round"]] = df_fixtures["league.round"].str.split(
        " - ", n=1, expand=True
    )

    # Convert numeric values to integers
    df_fixtures["goals.home"] = df_fixtures["goals.home"].astype("Int64")
    df_fixtures["goals.away"] = df_fixtures["goals.away"].astype("Int64")
    df_fixtures["score.halftime.home"] = df_fixtures["score.halftime.home"].astype(
        "Int64"
    )
    df_fixtures["score.halftime.away"] = df_fixtures["score.halftime.away"].astype(
        "Int64"
    )

    df_fixtures["fixture.referee"] = df_fixtures["fixture.referee"].apply(
        lambda referee_name: (
            Referee.map_referee_name(referee_name) if referee_name else None
        )
    )

    final_df = (
        df_fixtures.rename(
            columns={
                "fixture.id": "fixture_id",
                "fixture.date": "date",
                "fixture.referee": "referee",
                "fixture.status.short": "status",
                "league.id": "league_id",
                "league.name": "league_name",
                "league.country": "country_name",
                "league.season": "season_year",
                "teams.home.id": "home_team_id",
                "teams.home.name": "home_team_name",
                "teams.away.id": "away_team_id",
                "teams.away.name": "away_team_name",
                "goals.home": "goals_home",
                "goals.away": "goals_away",
                "score.halftime.home": "goals_home_ht",
                "score.halftime.away": "goals_away_ht",
            }
        )
        .filter(items=[column.name for column in Fixture.__table__.columns])
        .sort_values("date", ascending=True)
    )
    return final_df


def parse_coaches() -> pd.DataFrame:
    logging.info("** Parsing coaches data **")
    raw_df = load_all_files_from_directory("coaches")
    coaches_df = pd.DataFrame(
        [
            {
                "coach_id": row["id"],
                "coach_name": f"{row['firstname']} {row['lastname']}",
                "age": row["age"],
                "nationality": row["nationality"],
                "team_id": entry["team"]["id"],
                "team_name": entry["team"]["name"],
                "start_date": entry["start"],
                "end_date": entry["end"],
            }
            for _, row in raw_df.iterrows()
            for entry in row["career"]
        ],
        columns=[column.name for column in Coach.__table__.columns],
    )

    # Get data from 2016 until today
    coaches_df = coaches_df[
        (coaches_df["end_date"] >= "2016-01-01") | (coaches_df["end_date"].isnull())
    ]

    coaches_df.drop_duplicates(inplace=True)

    # Step 1: Check for coaches with more than one null in end_date
    null_count = coaches_df["end_date"].isnull().groupby(coaches_df["team_id"]).sum()
    multiple_nulls_coaches = null_count[null_count > 1].index

    # Step 2: Process each coach with multiple nulls
    results = []

    for team_id in multiple_nulls_coaches:
        coach_records = coaches_df[coaches_df["team_id"] == team_id]

        # Get the latest start_date
        latest_start_date = coach_records["start_date"].max()

        # Step 3: Fill null end_date with the day before the latest start_date
        for index, row in coach_records.iterrows():
            if pd.isnull(row["end_date"]) and row["start_date"] < latest_start_date:
                new_end_date = dt.datetime.strptime(
                    latest_start_date, "%Y-%m-%d"
                ) - pd.Timedelta(days=1)
                results.append(row.to_dict())
                results[-1]["end_date"] = new_end_date.date()
            else:
                results.append(row.to_dict())

    # Step 4: Create DataFrame from results
    result_df = pd.DataFrame(results)

    # Step 5: Keep original records with only one null in end_date
    single_nulls_df = coaches_df[~coaches_df["team_id"].isin(multiple_nulls_coaches)]

    # Step 6: Combine the two DataFrames
    final_df = pd.concat([result_df, single_nulls_df], ignore_index=True).sort_values(
        by="coach_id"
    )

    final_df["coach_id"] = final_df["coach_id"].astype("Int64")
    final_df["coach_name"] = final_df["coach_name"].apply(utf8_to_ascii)
    final_df["age"] = final_df["age"].astype(pd.Int64Dtype())
    final_df["team_id"] = final_df["team_id"].astype("Int64")

    # Remove rows where coach_name = None
    final_df = final_df[final_df["coach_name"] != "None None"]
    # Remove rows where start_date = end_date
    final_df = final_df[final_df["start_date"] != final_df["end_date"]]
    # Remove rows where without team_id
    final_df = final_df[final_df["team_id"] != 0]
    # Remove worse entry for valid duplicates
    final_df = final_df[
        ~((final_df["coach_id"] == 492) & (final_df["end_date"] == "2023-02-01"))
    ]
    # Remove worse entry for valid duplicates
    final_df = final_df[
        ~((final_df["coach_id"] == 9964) & (final_df["end_date"] == "2007-05-01"))
    ]
    # Remove worse entry for valid duplicates
    final_df = final_df[
        ~((final_df["coach_id"] == 14206) & (final_df["end_date"] == "2022-08-01"))
    ]

    return final_df.sort_values("coach_id", ascending=True)


def parse_events() -> pd.DataFrame:
    logging.info("** Parsing fixture events data **")
    df_events = load_all_files_from_directory("events_original")
    return df_events


def parse_fixture_stats() -> pd.DataFrame:
    logging.info("** Parsing fixture stats data **")
    df_fixture_stats = load_all_files_from_directory("statistics_fixtures")
    return df_fixture_stats


def parse_fixture_player_stats() -> pd.DataFrame:
    logging.info("** Parsing fixture players stats data **")
    df_player_stats = load_all_files_from_directory("player_statistics")
    return df_player_stats
