import logging
import pandas as pd

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.exc import OperationalError

from config.entity_names import ANALYTICS_BREAKS_SCHEMA_NAME
from models.data.fixtures import Fixture


class Break(Fixture):
    __tablename__ = "breaks"
    __table_args__ = {"schema": ANALYTICS_BREAKS_SCHEMA_NAME}
    __mapper_args__ = {"concrete": True}

    fixture_id = Column(Integer, primary_key=True)
    league_id = Column(Integer, ForeignKey("dw_main.leagues.league_id"), nullable=False)
    country_name = Column(String, nullable=False)
    season_year = Column(String, nullable=False)
    league_name = Column(String, nullable=False)
    season_stage = Column(String, nullable=False)
    round = Column(String)
    date = Column(DateTime, nullable=False)
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

    @classmethod
    def get_breaks_team_stats_raw(cls) -> pd.DataFrame:
        with cls.db.get_session() as session:
            try:
                breaks_df = pd.read_sql_query(
                    session.query(Break).statement,
                    cls.db.engine,
                )

                # Filter rows where the date is greater than or equal to '2020-01-01'
                filtered_breaks_df = breaks_df[breaks_df["date"] >= "2020-01-01"]

                # Create the 'home' DataFrame
                home_df = filtered_breaks_df[
                    [
                        "league_id",
                        "home_team_id",
                        "home_team_name",
                        "season_year",
                        "season_stage",
                        "round",
                        "referee",
                        "date",
                        "goals_home",
                        "goals_away",
                    ]
                ].copy()
                home_df["side"] = "home"
                home_df["winner"] = home_df["goals_home"] > home_df["goals_away"]
                home_df["year"] = home_df["date"].dt.year
                home_df["month"] = home_df["date"].dt.month
                home_df["day"] = home_df["date"].dt.day
                home_df.rename(
                    columns={"home_team_id": "team_id", "home_team_name": "team_name"},
                    inplace=True,
                )
                home_df.drop(columns=["goals_home", "goals_away"], inplace=True)

                # Create the 'away' DataFrame
                away_df = filtered_breaks_df[
                    [
                        "league_id",
                        "away_team_id",
                        "away_team_name",
                        "season_year",
                        "season_stage",
                        "round",
                        "referee",
                        "date",
                        "goals_home",
                        "goals_away",
                    ]
                ].copy()
                away_df["winner"] = away_df["goals_home"] < away_df["goals_away"]
                away_df["side"] = "away"
                away_df["year"] = away_df["date"].dt.year
                away_df["month"] = away_df["date"].dt.month
                away_df["day"] = away_df["date"].dt.day
                away_df.rename(
                    columns={"away_team_id": "team_id", "away_team_name": "team_name"},
                    inplace=True,
                )
                away_df.drop(columns=["goals_home", "goals_away"], inplace=True)

                # Concatenate home_df and away_df
                result_df = pd.concat([home_df, away_df], ignore_index=True)
            except OperationalError as e:
                logging.error(f"OperationalError occurred: {str(e)}")
                raise  # This raises the original error
        return result_df
