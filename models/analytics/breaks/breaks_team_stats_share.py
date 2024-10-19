import logging
import pandas as pd
from sqlalchemy import Column, Integer, String, Date, Numeric, or_

from models.base import Base
from models.data.fixtures.fixtures import Fixture
from services.db import Db
from helpers.vars import CURRENT_UTC_DATETIME

db = Db()

# Specify the schema
SCHEMA_NAME = "analytics_breaks"


class BreaksTeamStatsShares(Base):
    __tablename__ = "breaks_team_stats_shares"
    __table_args__ = {"schema": SCHEMA_NAME}
    __mapper_args__ = {"concrete": True}

    team_id = Column(Integer, primary_key=True)
    team_name = Column(String, nullable=False)
    last_break = Column(Date, nullable=False)
    total = Column(Integer)
    home_share = Column(Numeric(4, 2))
    away_share = Column(Numeric(4, 2))
    jan_share = Column(Numeric(4, 2))
    feb_share = Column(Numeric(4, 2))
    mar_share = Column(Numeric(4, 2))
    apr_share = Column(Numeric(4, 2))
    may_share = Column(Numeric(4, 2))
    jun_share = Column(Numeric(4, 2))
    jul_share = Column(Numeric(4, 2))
    aug_share = Column(Numeric(4, 2))
    sep_share = Column(Numeric(4, 2))
    oct_share = Column(Numeric(4, 2))
    nov_share = Column(Numeric(4, 2))
    dec_share = Column(Numeric(4, 2))
    beg_month_share = Column(Numeric(4, 2))
    mid_month_share = Column(Numeric(4, 2))
    end_month_share = Column(Numeric(4, 2))
    c_2012_share = Column("2012_share", Numeric(4, 2))
    c_2013_share = Column("2013_share", Numeric(4, 2))
    c_2014_share = Column("2014_share", Numeric(4, 2))
    c_2015_share = Column("2015_share", Numeric(4, 2))
    c_2016_share = Column("2016_share", Numeric(4, 2))
    c_2017_share = Column("2017_share", Numeric(4, 2))
    c_2018_share = Column("2018_share", Numeric(4, 2))
    c_2019_share = Column("2019_share", Numeric(4, 2))
    c_2020_share = Column("2020_share", Numeric(4, 2))
    c_2021_share = Column("2021_share", Numeric(4, 2))
    c_2022_share = Column("2022_share", Numeric(4, 2))
    c_2023_share = Column("2023_share", Numeric(4, 2))
    c_2024_share = Column("2024_share", Numeric(4, 2))
    rounds_1_13_share = Column("rounds_1-13_share", Numeric(4, 2))
    rounds_14_26_share = Column("rounds_14-26_share", Numeric(4, 2))
    rounds_27_39_share = Column("rounds_27-39_share", Numeric(4, 2))
    rounds_40_60_share = Column("rounds_40-60_share", Numeric(4, 2))

    @classmethod
    def get_breaks_teams_points_for_fixture(
        cls, fixture_id, home_team_id, away_team_id, date, round, referee
    ) -> pd.DataFrame:
        with db.get_session() as session:
            team_shares = pd.read_sql_query(
                session.query(cls)
                .filter(or_(cls.team_id == home_team_id, cls.team_id == away_team_id))
                .statement,
                db.engine,
            )

        def calculate_home_away_factor(team_id) -> int:
            team_home_share = team_shares[team_shares["team_id"] == team_id][
                "home_share"
            ]
            team_away_share = team_shares[team_shares["team_id"] == team_id][
                "away_share"
            ]
            difference = team_home_share - team_away_share
            factor = difference * 0.1

            fctr = factor.reset_index(drop=True)

            if len(fctr) < 1:
                return 0

            return fctr[0]

        def get_month_part_factor(team_id, date) -> int:
            day = date.day
            month_part = None

            match day:
                case x if x < 11:
                    month_part = "beg_month"
                case x if x >= 11 & x < 21:
                    month_part = "mid_month"
                case x if x > 20:
                    month_part = "end_month"
                case _:
                    return 0

            factor = team_shares[team_shares["team_id"] == team_id][
                month_part + "_share"
            ]

            fctr = factor.reset_index(drop=True)

            if len(fctr) < 1:
                return 0

            return fctr[0]

        def get_month_factor(team_id, date) -> int:
            month = date.month
            months = {
                1: "jan",
                2: "feb",
                3: "mar",
                4: "apr",
                5: "may",
                6: "jun",
                7: "jul",
                8: "aug",
                9: "sep",
                10: "oct",
                11: "nov",
                12: "dec",
            }
            factor = team_shares[team_shares["team_id"] == team_id][
                str(months.get(month)) + "_share"
            ]

            fctr = factor.reset_index(drop=True)

            if len(fctr) < 1:
                return 0

            return fctr[0]

        def get_year_factor():
            return 0

        def get_round_factor(team_id: int, round: int) -> int:
            if not round or len(round) > 2:
                return 0

            round = int(round)

            match round:
                case x if x < 14:
                    season_part = "rounds_1-13"
                case x if x > 14 & x < 27:
                    season_part = "rounds_14-26"
                case x if x > 27 & x < 40:
                    season_part = "rounds_27-39"
                case x if x > 40 & x < 61:
                    season_part = "rounds_40-60"
                case _:
                    return 0

            factor = team_shares[team_shares["team_id"] == team_id][
                season_part + "_share"
            ]

            fctr = factor.reset_index(drop=True)

            if len(fctr) < 1:
                return 0

            return fctr[0]

        def get_referee_factor(team_id, referee) -> int:
            return 0

        def get_total_factor(team_id) -> int:
            total_factor = (
                calculate_home_away_factor(team_id)
                + get_month_part_factor(team_id, date)
                + get_month_factor(team_id, date)
                + get_year_factor()
                + get_round_factor(team_id, round)
                + get_referee_factor(team_id, referee)
            )

            return total_factor

        home_team_factor = get_total_factor(home_team_id)
        away_team_factor = get_total_factor(away_team_id)

        breaks_teams_points_for_fixture_df = pd.DataFrame(
            [
                {
                    "fixture_id": fixture_id,
                    "home_team_id": home_team_id,
                    "away_team_id": away_team_id,
                    "home_team_factor": home_team_factor,
                    "away_team_factor": away_team_factor,
                    "total_factor": home_team_factor + away_team_factor,
                }
            ]
        )

        return breaks_teams_points_for_fixture_df

    # TODO: this method could depend on input_df instead of being only for breaks
    @classmethod
    def get_breaks_with_factors(cls) -> pd.DataFrame:
        breaks_with_factors_df = pd.DataFrame()
        list_of_dfs = []
        with db.get_session() as session:
            try:
                # TODO: original value = breaks_df (not current_year_fixtures_df)
                current_year_fixtures_df = pd.read_sql_query(
                    session.query(Fixture)
                    .filter(Fixture.date > "2024-01-01")
                    .statement,
                    db.engine,
                )
                for idx, single_break in current_year_fixtures_df.iterrows():
                    break_with_factors_df = cls.get_breaks_teams_points_for_fixture(
                        single_break["fixture_id"],
                        single_break["home_team_id"],
                        single_break["away_team_id"],
                        single_break["date"],
                        single_break["round"],
                        single_break["referee"],
                    )
                    temp_df = pd.concat([breaks_with_factors_df, break_with_factors_df])
                    list_of_dfs.append(temp_df)
                combined_df = pd.concat(list_of_dfs, ignore_index=True)

                # Get total breaks data for each team
                total_breaks_per_team_df = pd.read_sql_query(
                    session.query(cls.team_id, cls.total).statement,
                    db.engine,
                )

                fixtures_with_total_home_breaks_df = current_year_fixtures_df.merge(
                    total_breaks_per_team_df,
                    left_on="home_team_id",
                    right_on="team_id",
                    how="left",
                )
                fixtures_with_total_home_and_away_breaks_df = (
                    fixtures_with_total_home_breaks_df.merge(
                        total_breaks_per_team_df,
                        left_on="away_team_id",
                        right_on="team_id",
                        how="left",
                    )
                )

                breaks_with_factors_final_df = (
                    fixtures_with_total_home_and_away_breaks_df.merge(
                        combined_df, on="fixture_id", how="left"
                    )
                    .filter(
                        items=[
                            "fixture_id",
                            "date",
                            "home_team_name",
                            "away_team_name",
                            "goals_home",
                            "goals_away",
                            "goals_home_ht",
                            "goals_away_ht",
                            "home_team_factor",
                            "away_team_factor",
                            "total_factor",
                            "total_x",
                            "total_y",
                        ]
                    )
                    .rename(
                        columns={
                            "total_x": "home_team_breaks",
                            "total_y": "away_team_breaks",
                        }
                    )
                )

                breaks_with_factors_final_df["goals_home"] = (
                    breaks_with_factors_final_df["goals_home"].fillna(0).astype(int)
                )
                breaks_with_factors_final_df["goals_away"] = (
                    breaks_with_factors_final_df["goals_away"].fillna(0).astype(int)
                )
                breaks_with_factors_final_df["goals_home_ht"] = (
                    breaks_with_factors_final_df["goals_home_ht"].fillna(0).astype(int)
                )
                breaks_with_factors_final_df["goals_away_ht"] = (
                    breaks_with_factors_final_df["goals_away_ht"].fillna(0).astype(int)
                )
                breaks_with_factors_final_df["home_team_breaks"] = (
                    breaks_with_factors_final_df["home_team_breaks"]
                    .fillna(0)
                    .astype(int)
                )
                breaks_with_factors_final_df["away_team_breaks"] = (
                    breaks_with_factors_final_df["away_team_breaks"]
                    .fillna(0)
                    .astype(int)
                )

                return breaks_with_factors_final_df
            except Exception as e:
                raise e
