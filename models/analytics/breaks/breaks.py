import pandas as pd
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey

from models.data.fixtures.fixtures import Fixture
from services.db import Db

db = Db()

# Specify the schema
SCHEMA_NAME = "analytics_breaks"


class Break(Fixture):
    __tablename__ = "breaks"
    __table_args__ = {"schema": SCHEMA_NAME}
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

    @staticmethod
    def get_breaks_team_stats_raw():
        with db.get_session() as session:
            try:
                sql_stmnt = """
                                WITH home AS (
                                                SELECT
                                                        league_id,
                                                        home_team_id AS team_id,
                                                        home_team_name AS team_name,
                                                        season_year,
                                                        season_stage,
                                                        round,
                                                        referee,
                                                        'home' AS side,
                                                        EXTRACT(YEAR FROM date) AS year,
                                                        EXTRACT(MONTH FROM date) AS month,
                                                        EXTRACT(DAY FROM date) AS day
                                                FROM
                                                        analytics_breaks.breaks
                                ),
                                away AS (
                                                SELECT
                                                        league_id,
                                                        away_team_id AS team_id,
                                                        away_team_name AS team_name,
                                                        season_year,
                                                        season_stage,
                                                        round,
                                                        referee,
                                                        'away' AS side,
                                                        EXTRACT(YEAR FROM date) AS year,
                                                        EXTRACT(MONTH FROM date) AS month,
                                                        EXTRACT(DAY FROM date) AS day
                                                FROM
                                                        analytics_breaks.breaks
                                )
                                SELECT * FROM home
                                UNION
                                SELECT * FROM away
                """
                breaks_team_stats_raw = session.execute(sql_stmnt).all()
                break_team_stats_raw_df = pd.DataFrame(
                    breaks_team_stats_raw,
                    columns=[
                        "league_id",
                        "team_id",
                        "team_name",
                        "season_year",
                        "season_stage",
                        "round",
                        "referee",
                        "side",
                        "year",
                        "month",
                        "day",
                    ],
                )
                return break_team_stats_raw_df
            except Exception:
                raise Exception
