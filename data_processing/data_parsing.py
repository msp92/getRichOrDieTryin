import datetime as dt
import logging
import numpy as np
import pandas as pd

from config.entity_names import (
    FIXTURE_PLAYER_STATS_DIR,
    FIXTURE_STATS_DIR,
    FIXTURES_DIR,
    COUNTRIES_DIR,
    LEAGUES_DIR,
    COUNTRIES_FILE_NAME,
    LEAGUES_FILE_NAME,
    TEAMS_DIR,
)
from data_processing.data_processing import load_all_files_from_data_directory
from data_processing.data_transformations import adjust_date_range_overlaps

from helpers.utils import get_df_from_json, utf8_to_ascii
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)


def parse_countries() -> pd.DataFrame:
    logging.info("** Parsing countries data **")
    df = get_df_from_json(COUNTRIES_FILE_NAME, COUNTRIES_DIR).rename(
        columns={"name": "country_name"}
    )
    return df


def parse_leagues() -> pd.DataFrame:
    from models.data_warehouse.main import Country, League

    logging.info("** Parsing leagues data **")
    country_df = Country.get_df_from_table()
    df = get_df_from_json(LEAGUES_FILE_NAME, LEAGUES_DIR).rename(
        columns={
            "league.id": "league_id",
            "league.name": "league_name",
            "league.type": "type",
            "league.logo": "logo",
            "country.name": "country_name",
        }
    )
    final_df = (
        pd.merge(df, country_df, on="country_name", how="left")
        .filter(items=[column.name for column in League.__table__.columns])
        .sort_values(by="league_id")
    )
    return final_df


def parse_teams() -> pd.DataFrame:
    from models.data_warehouse.main import Country, Team

    logging.info("** Parsing teams data **")
    country_df = Country.get_df_from_table()
    df = load_all_files_from_data_directory(TEAMS_DIR)
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
    final_df = final_df.dropna(subset=["country_id"])
    final_df["country_id"] = final_df["country_id"].astype(int)
    return final_df


def parse_seasons() -> pd.DataFrame:
    from models.data_warehouse.main import Country, Season

    logging.info("** Parsing seasons data **")
    leagues_df = get_df_from_json(LEAGUES_FILE_NAME, LEAGUES_DIR)
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


def parse_fixtures() -> pd.DataFrame:
    from models.data_warehouse.main import Referee
    from models.data_warehouse.fixtures import Fixture

    logging.info("** Parsing fixtures data **")
    df_fixtures = load_all_files_from_data_directory(f"{FIXTURES_DIR}")

    # TODO: check for duplicates; if Yes -> take status FT/AET/PEN
    # TODO: for now try without status = 'PST'
    df_fixtures.drop_duplicates(inplace=True)
    df_fixtures = df_fixtures[df_fixtures["fixture.status.short"] != "PST"]

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
    from models.data_warehouse.main import Coach

    logging.info("** Parsing coaches data **")
    raw_df = load_all_files_from_data_directory("coaches")
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

    # Sortowanie daty początkowej i resetowanie indeksu
    coaches_df = coaches_df.sort_values(
        by=["coach_id", "team_id", "start_date", "end_date"],
        ascending=[True, True, True, False],
    ).reset_index(drop=True)

    # Usuwanie rekordów z wcześniejszą datą 'end_date' w przypadku duplikatów w (coach_id, team_id, start_date)
    coaches_df = coaches_df.drop_duplicates(
        subset=["coach_id", "team_id", "start_date"], keep="first"
    )

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
    combined_df = pd.concat(
        [result_df, single_nulls_df], ignore_index=True
    ).sort_values(by="coach_id")

    # Step 7: Apply the adjustment function to each group of B
    final_df = combined_df.groupby("team_id", group_keys=False).apply(
        adjust_date_range_overlaps
    )

    final_df["coach_id"] = final_df["coach_id"].astype("Int64")
    final_df["coach_name"] = final_df["coach_name"].apply(utf8_to_ascii)
    final_df["age"] = final_df["age"].astype(pd.Int64Dtype())
    final_df["team_id"] = final_df["team_id"].astype("Int64")

    # Remove rows where coach_name = None
    final_df = final_df[final_df["coach_name"] != "None None"]
    # Remove rows where start_date >= end_date
    final_df = final_df[
        (final_df["start_date"] < final_df["end_date"]) | (final_df["end_date"].isna())
    ]
    # Remove rows without team_id
    final_df = final_df[final_df["team_id"] != 0]
    # Remove worse entries for valid duplicates
    conditions = [
        (final_df["coach_id"] == 492) & (final_df["end_date"] == "2023-02-01"),
        (final_df["coach_id"] == 9964) & (final_df["end_date"] == "2007-05-01"),
        (final_df["coach_id"] == 14206) & (final_df["end_date"] == "2022-08-01"),
    ]
    final_df = final_df[~(conditions[0] | conditions[1] | conditions[2])]
    return final_df.sort_values("coach_id", ascending=True)


def parse_fixture_events_file(file_name: str) -> pd.DataFrame:
    # logging.info("** Parsing fixture events data **")
    # raw_df = load_all_files_from_data_directory("events")
    raw_df = get_df_from_json(file_name[:-5], sub_dir="fixture_events")
    raw_df.fillna("", inplace=True)
    final_df = raw_df.rename(
        columns={
            "time.elapsed": "elapsed_time",
            "time.extra": "extra_time",
            "type": "event_type",
            "detail": "event_detail",
            "team.id": "team_id",
            "team.name": "team_name",
        }
    )[
        [
            "fixture_id",
            "event_id",
            "elapsed_time",
            "extra_time",
            "event_type",
            "event_detail",
            "team_id",
            "team_name",
        ]
    ]
    final_df["extra_time"] = final_df["extra_time"].replace(
        ["", None, float("nan")], pd.NA
    )
    final_df["extra_time"] = final_df["extra_time"].astype(pd.Int64Dtype())
    final_df = final_df[final_df["event_type"] != "subst"]
    return final_df


def parse_fixture_stats_file(file_name: str) -> pd.DataFrame:
    # logging.info("** Parsing fixture stats data **")
    raw_df = get_df_from_json(file_name[:-5], sub_dir=FIXTURE_STATS_DIR)
    # raw_df = load_all_files_from_data_directory("fixture_stats")
    # Replace None and NaN values with a placeholder value to avoid None/NaN values in
    raw_df.fillna("", inplace=True)
    # raw_df["side"] = ["home", "away"]
    # Transform from {"key": "Shots On Goal", "value": 5} to {"Shots On Goal": 10}
    raw_df["statistics"] = raw_df["statistics"].apply(
        lambda x: {d["type"]: d["value"] for d in x}
    )
    final_df = raw_df.rename(columns={"team.id": "team_id", "team.name": "team_name"})[
        ["fixture_id", "side", "team_id", "team_name", "statistics"]
    ]
    return final_df


def parse_fixture_player_stats_file(file_name: str) -> pd.DataFrame:
    # logging.info("** Parsing fixture players stats data **")
    # raw_df = load_all_files_from_data_directory("player_stats")
    raw_df = get_df_from_json(file_name[:-5], sub_dir=FIXTURE_PLAYER_STATS_DIR)
    raw_df.fillna("", inplace=True)
    # raw_df["side"] = ["home", "away"]
    # Transform statistics to key:value pairs
    raw_df["players"] = raw_df["players"].apply(
        lambda players: [
            {
                "player_id": player["player"]["id"],
                "player_name": player["player"]["name"],
                "statistics": {
                    stat: value
                    for stats in player["statistics"]
                    for stat, value in stats.items()
                },
            }
            for player in players
        ]
    )
    # Explode the 'players' column to create a row for each player in each row of df
    raw_df = raw_df.explode("players", ignore_index=True)

    # Convert the dictionaries in 'players' into separate columns
    raw_df = pd.concat(
        [raw_df.drop(columns=["players"]), raw_df["players"].apply(pd.Series)], axis=1
    )
    final_df = raw_df.rename(columns={"team.id": "team_id", "team.name": "team_name"})[
        [
            "fixture_id",
            "side",
            "team_id",
            "team_name",
            "player_id",
            "player_name",
            "statistics",
        ]
    ]
    # Remove rows without player_id
    final_df = final_df[final_df["player_id"] != 0]
    return final_df
