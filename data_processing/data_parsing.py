import time
import pandas as pd
from data_processing.data_processing import load_all_files_from_directory
from models.countries import Country
from models.fixtures import Fixture
from models.leagues import League
from models.seasons import Season
from models.teams import Team
from utils.utils import get_df_from_json


def parse_countries() -> pd.DataFrame:
    print(f"\n** Parsing countries data **")
    df = get_df_from_json("countries").rename(columns={"name": "country_name"})
    return df


def parse_leagues() -> pd.DataFrame:
    print(f"\n** Parsing leagues data **")
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
    final_df = pd.merge(df, country_df, on="country_name", how="left").filter(
        items=League.get_columns_list()
    ).sort_values(by="league_id")
    return final_df


def parse_teams():
    print(f"\n** Parsing teams data **")
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
    final_df = pd.merge(df, country_df, on="country_name", how="left").filter(
        items=Team.get_columns_list()
    ).sort_values(by="team_id")
    return final_df


def parse_seasons() -> pd.DataFrame:
    print(f"\n** Parsing seasons data **")
    leagues_df = get_df_from_json("leagues")
    country_df = Country.get_df_from_table()
    # Explode 'season' column and create new columns from season dict values
    final_df = (
        pd.concat(
            [
                leagues_df.drop(columns="seasons"),
                leagues_df["seasons"].explode().apply(pd.Series),
            ],
            axis=1,
        ).rename(columns={"league.id": "league_id", "country.name": "country_name"})
    ).sort_values("league_id", ascending=True)
    # Convert to date and cut timestamp
    final_df["start_date"] = pd.to_datetime(
        final_df["start"], format="%Y-%m-%d"
    ).dt.date
    final_df["end_date"] = pd.to_datetime(final_df["end"], format="%Y-%m-%d").dt.date

    # Create id's in L1_S2022 format
    final_df["season_id"] = (
        "L" + final_df["league_id"].astype(str) + "_S" + final_df["year"].astype(str)
    )

    final_df = pd.merge(final_df, country_df, on="country_name", how="left")
    final_df = final_df.filter(items=Season.get_columns_list())
    return final_df


def parse_fixtures(subdir: str) -> pd.DataFrame:
    """
        Parse fixtures data from JSON files in a specified subdirectory and return a DataFrame.

        Args:
            subdir (str): The subdirectory containing JSON files with fixtures data.

        Returns:
            pd.DataFrame: A DataFrame containing parsed fixtures data.

    """
    print(f"\n** Parsing fixtures data **")
    df_fixtures = load_all_files_from_directory(f"fixtures/{subdir}")
    df_fixtures["season_id"] = (
        "L" + df_fixtures["league.id"].astype(str) + "_S" + df_fixtures["league.season"].astype(str)
    )

    df_fixtures["goals.home"] = df_fixtures["goals.home"].astype("Int64")
    df_fixtures["goals.away"] = df_fixtures["goals.away"].astype("Int64")
    df_fixtures["score.halftime.home"] = df_fixtures["score.halftime.home"].astype("Int64")
    df_fixtures["score.halftime.away"] = df_fixtures["score.halftime.away"].astype("Int64")

    final_df = df_fixtures.rename(
        columns={
            "fixture.id": "fixture_id",
            "fixture.date": "date",
            "fixture.referee": "referee",
            "fixture.status.short": "status",
            "league.id": "league_id",
            "league.name": "league_name",
            "league.country": "country_name",
            "league.season": "season_year",
            "league.round": "round",
            "teams.home.id": "home_team_id",
            "teams.home.name": "home_team_name",
            "teams.away.id": "away_team_id",
            "teams.away.name": "away_team_name",
            "goals.home": "goals_home",
            "goals.away": "goals_away",
            "score.halftime.home": "goals_home_ht",
            "score.halftime.away": "goals_away_ht",
        }
    ).filter(items=Fixture.get_columns_list()).sort_values("date", ascending=True)
    return final_df


def parse_events() -> pd.DataFrame:
    print(f"\n** Parsing fixture events data **")
    df_events = load_all_files_from_directory(f"events_original")
    return df_events


def parse_fixture_stats() -> pd.DataFrame:
    print(f"\n** Parsing fixture stats data **")
    df_fixture_stats = load_all_files_from_directory(f"statistics_fixtures")
    dups = df_fixture_stats[df_fixture_stats.duplicated(subset=['fixture_id'], keep=False)]
    return df_fixture_stats


def parse_fixture_player_stats() -> pd.DataFrame:
    print(f"\n** Parsing fixture players stats data **")
    df_player_stats = load_all_files_from_directory(f"player_statistics")
    return df_player_stats
