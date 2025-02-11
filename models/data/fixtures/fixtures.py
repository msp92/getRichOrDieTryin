import logging

import pandas as pd
import datetime as dt

from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
    func,
    asc,
    DateTime,
)
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm import relationship
from models.data.main.leagues import League  # NOQA: F401
from config.vars import DATA_DIR
from models.base import Base
from services.db import Db
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))


db = Db()

# Specify the schema
SCHEMA_NAME = "dw_fixtures"


class Fixture(Base):
    __tablename__ = "fixtures"
    __table_args__ = {"schema": SCHEMA_NAME}

    fixture_id = Column(Integer, primary_key=True)
    league_id = Column(Integer, ForeignKey("dw_main.leagues.league_id"), nullable=False)
    country_name = Column(String, nullable=False)
    season_year = Column(String, nullable=False)
    league_name = Column(String, nullable=False)
    season_stage = Column(String, nullable=False)
    round = Column(String)
    date = Column(
        DateTime, nullable=False
    )  # TODO: convert to Column(DateTime(timezone=True))
    status = Column(String, nullable=False)
    referee = Column(String)
    home_team_id = Column(Integer, ForeignKey("dw_main.teams.team_id"), nullable=False)
    home_team_name = Column(String, nullable=False)
    away_team_id = Column(Integer, ForeignKey("dw_main.teams.team_id"), nullable=False)
    away_team_name = Column(String, nullable=False)
    goals_home = Column(Integer)
    goals_away = Column(Integer)
    goals_home_ht = Column(Integer)
    goals_away_ht = Column(Integer)
    update_date = Column(DateTime)

    league = relationship("League", back_populates="fixture")

    home_team = relationship(
        "Team", foreign_keys=[home_team_id], back_populates="home_team"
    )
    away_team = relationship(
        "Team", foreign_keys=[away_team_id], back_populates="away_team"
    )

    @staticmethod
    def get_dates_to_update() -> list[str]:
        curr_date = dt.date.today()
        start_date = curr_date - dt.timedelta(days=2)
        end_date = curr_date + dt.timedelta(days=2)

        # Generate list of dates as strings
        date_range = [
            (start_date + dt.timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range((end_date - start_date).days + 1)
        ]
        return date_range

    @classmethod
    def calculate_share_of_not_started_games(cls, date_to_check: str) -> int:
        with db.get_session() as session:
            try:
                all_fixture_count: int = (
                    session.query(func.count(cls.fixture_id))
                    .filter(func.DATE(cls.date) == date_to_check)
                    .scalar()
                )
                not_started_fixture_count: int = (
                    session.query(func.count(cls.fixture_id))
                    .filter(
                        (func.DATE(cls.date) == date_to_check) & (cls.status == "NS")
                    )
                    .scalar()
                )
            except Exception as e:
                logging.error(f"{e}")
        return int(not_started_fixture_count / all_fixture_count * 100)

    @classmethod
    def get_results_max_date(cls) -> str:
        """
        Retrieve the maximum date of results in the database.

        This method queries the database to find the maximum date of results
        where the status is 'FT' (full time).

        Returns:
            str: The maximum date of results in the format 'YYYY-MM-DD'.

        Raises:
            Exception: If an error occurs during the database operation.
        """
        with db.get_session() as session:
            try:
                max_date = (
                    session.query(func.max(cls.date))
                    .filter(cls.status == "FT")
                    .scalar()
                )
                return max_date.strftime("%Y-%m-%d")
            except Exception:
                raise Exception

    @classmethod
    def get_today_fixtures(cls) -> pd.DataFrame:
        """
        Retrieve the maximum date of results in the database.

        This method queries the database to find fixtures that
        were played or will be played today.

        Returns:
            pd.DataFrame: Dataframe consists of today's fixtures.

        Raises:
            Exception: If an error occurs during the database operation.
        """
        with db.get_session() as session:
            try:
                today_fixtures_df = pd.read_sql_query(
                    session.query(cls).filter(cls.date == dt.date.today()).statement,
                    db.engine,
                )
                return today_fixtures_df
            except Exception:
                raise Exception

    @classmethod
    def get_season_fixtures_by_team(
        cls, team_id: int, season_year: str, status: str = "ALL"
    ) -> pd.DataFrame:
        """
        Retrieve fixtures of a specific team for a given season and status.
        Friendly games excluded.

        Args:
            team_id (int): The ID of the team.
            season_year (str): The year of the season (e.g., "2023").
            status (str): The status of the fixtures to retrieve.
                          Options are "ALL", "FT" (Finished) and "NS" (Not Started).

        Returns:
            pd.DataFrame: A DataFrame containing the fixtures of the specified team for the given season and status.

        Raises:
            Exception: If there's any error during the database query or processing.
        """
        with db.get_session() as session:
            try:
                team_fixtures_df = pd.read_sql_query(
                    session.query(cls)
                    .filter(
                        (cls.season_year == season_year)
                        & (
                            (cls.home_team_id == team_id)
                            | (cls.away_team_id == team_id)
                        )
                        & (cls.league_id != 667)  # Excluding Friendlies
                    )
                    .statement,
                    db.engine,
                )
                # Return all except Not Started - most statuses are for finished games
                if status == "FT":
                    team_fixtures_df = team_fixtures_df[
                        team_fixtures_df["status"] != "NS"
                    ]
                elif status == "NS":
                    team_fixtures_df = team_fixtures_df[
                        team_fixtures_df["status"] == "NS"
                    ]
                return team_fixtures_df
            except Exception:
                raise Exception

    @staticmethod
    def filter_fixtures_by_rounds(df: pd.DataFrame, rounds: str | int) -> pd.DataFrame:
        match rounds:
            case "all_finished":
                return df[df["status"] == "FT"]
            case "last_5":
                return df[df["status"] == "FT"].tail(5)
            case int():
                # If rounds is a number filter only "Regular Season" fixtures
                df = df[df["round"].str.contains("Regular Season")]
                return df[
                    df["round"].str.split("-").str[1].str.strip().astype(int) <= rounds
                ]

    @classmethod
    def get_upcoming_fixtures_by_team(
        cls, team_id: int, season_year: str
    ) -> pd.DataFrame:
        team_upcoming_fixtures_df = cls.get_season_fixtures_by_team(
            team_id, season_year, "NS"
        )
        return team_upcoming_fixtures_df

    @classmethod
    def get_season_stats_by_team(cls, team_id: int, season_year: str) -> pd.DataFrame:
        team_results_df = cls.get_season_fixtures_by_team(team_id, season_year, "FT")

        # Create grouping subsets: home, away
        team_results_df["team_group"] = team_results_df.apply(
            lambda row: "home" if row["home_team_id"] == team_id else "away", axis=1
        )
        team_results_df["team_name"] = team_results_df.apply(
            lambda row: (
                row["home_team_name"]
                if row["team_group"] == "home"
                else row["away_team_name"]
            ),
            axis=1,
        )
        team_results_df = team_results_df.sort_values(by="date")

        # Add 'form' column to the DataFrame
        team_results_df["form"] = team_results_df.apply(
            lambda row: (
                "W"
                if (
                    row["team_group"] == "home"
                    and row["goals_home"] > row["goals_away"]
                )
                or (
                    row["team_group"] == "away"
                    and row["goals_away"] > row["goals_home"]
                )
                else "D" if row["goals_home"] == row["goals_away"] else "L"
            ),
            axis=1,
        )

        # TODO: make similar col like above but for goals_scored and conceded

        team_stats_grouped = (
            team_results_df.groupby(["team_group", "team_name"])
            .agg(
                games=("fixture_id", "count"),
                wins=("form", lambda x: (x == "W").sum()),
                draws=("form", lambda x: (x == "D").sum()),
                loses=("form", lambda x: (x == "L").sum()),
                # goals_scored=("goals_home" if "team_group" == "home" else "goals_away", "sum"),  # TODO: why it doesn't sum correctly??
                # goals_conceded=("goals_away" if "team_group" == "home" else "goals_home", "sum"),  # TODO: why it doesn't sum correctly??
                # avg_goals_scored=("goals_home", lambda x: x.mean()),
                # avg_goals_conceded=("goals_away", lambda x: x.mean()),
                form=("form", lambda x: "".join(x)),
            )
            .reset_index()
        )
        # TODO: make it much shorter
        team_stats_grouped.loc[
            team_stats_grouped["team_group"] == "home", "goals_scored"
        ] = team_results_df.loc[
            team_results_df["team_group"] == "home", "goals_home"
        ].sum()
        team_stats_grouped.loc[
            team_stats_grouped["team_group"] == "away", "goals_scored"
        ] = team_results_df.loc[
            team_results_df["team_group"] == "away", "goals_away"
        ].sum()
        team_stats_grouped.loc[
            team_stats_grouped["team_group"] == "home", "goals_conceded"
        ] = team_results_df.loc[
            team_results_df["team_group"] == "home", "goals_away"
        ].sum()
        team_stats_grouped.loc[
            team_stats_grouped["team_group"] == "away", "goals_conceded"
        ] = team_results_df.loc[
            team_results_df["team_group"] == "away", "goals_home"
        ].sum()
        team_stats_grouped["avg_goals_scored"] = (
            team_stats_grouped["goals_scored"] / team_stats_grouped["games"]
        )
        team_stats_grouped["avg_goals_conceded"] = (
            team_stats_grouped["goals_conceded"] / team_stats_grouped["games"]
        )

        total_row = pd.DataFrame(
            {
                "team_group": "total",
                "team_name": team_stats_grouped.iloc[0]["team_name"],
                "games": team_stats_grouped.iloc[0]["games"]
                + team_stats_grouped.iloc[1]["games"],
                "wins": team_stats_grouped.iloc[0]["wins"]
                + team_stats_grouped.iloc[1]["wins"],
                "draws": team_stats_grouped.iloc[0]["draws"]
                + team_stats_grouped.iloc[1]["draws"],
                "loses": team_stats_grouped.iloc[0]["loses"]
                + team_stats_grouped.iloc[1]["loses"],
                "goals_scored": int(
                    team_stats_grouped.iloc[0]["goals_scored"]
                    + team_stats_grouped.iloc[1]["goals_scored"]
                ),
                "goals_conceded": int(
                    team_stats_grouped.iloc[0]["goals_conceded"]
                    + team_stats_grouped.iloc[1]["goals_conceded"]
                ),
            },
            index=[0],
        )
        total_row["avg_goals_scored"] = total_row["goals_scored"] / total_row["games"]
        total_row["avg_goals_conceded"] = (
            total_row["goals_conceded"] / total_row["games"]
        )
        total_row["form"] = "".join(team_results_df["form"])

        team_stats_df = pd.concat([team_stats_grouped, total_row]).reset_index(
            drop=True
        )
        team_stats_df["avg_goals_scored"] = team_stats_df["avg_goals_scored"].apply(
            lambda x: round(x, 3)
        )
        team_stats_df["avg_goals_conceded"] = team_stats_df["avg_goals_conceded"].apply(
            lambda x: round(x, 3)
        )
        return team_stats_df

    @classmethod
    def create_game_preview(cls, home_team_id: int, away_team_id: int) -> pd.DataFrame:
        home_team_stats = cls.get_season_stats_by_team(home_team_id, "2023")
        away_team_stats = cls.get_season_stats_by_team(away_team_id, "2023")

        timestamp = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        game_preview_df = pd.concat([home_team_stats, away_team_stats]).reset_index(
            drop=True
        )
        game_preview_df.to_csv(
            f"{DATA_DIR}/previews/{timestamp}_{home_team_id}-{away_team_id}.csv",
            index=False,
        )
        return game_preview_df

    @classmethod
    def get_breaks(cls) -> pd.DataFrame:
        overcome_mask = (
            (cls.goals_home_ht > cls.goals_away_ht)
            & (cls.goals_home < cls.goals_away)
            & (cls.status == "FT")
        ) | (
            (cls.goals_home_ht < cls.goals_away_ht)
            & (cls.goals_home > cls.goals_away)
            & (cls.status == "FT")
        )

        with db.get_session() as session:
            try:
                overcome_games_df = pd.read_sql_query(
                    session.query(cls)
                    .filter(overcome_mask)
                    .order_by(asc(cls.date))
                    .statement,
                    db.engine,
                )
            except InvalidRequestError as e:
                raise InvalidRequestError(
                    f"Error while reading {cls.__name__} data: {e}"
                )
        return overcome_games_df.drop(columns=["update_date"])
