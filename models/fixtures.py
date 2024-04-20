from sqlalchemy import (
    Column,
    Integer,
    String,
    Date,
    ForeignKey,
    func,
)
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy import or_
from datetime import date, datetime
from config.config import SOURCE_DIR
from models.base import Base
from services.db import get_engine
import pandas as pd


class Fixture(Base):
    __tablename__ = "fixtures"

    def __repr__(self):
        """Return a string representation of the User object."""
        return (
            f"<Fixture(fixture_id={self.fixture_id}, league_id={self.league_id}, season_id={self.season_id},"
            f"country_name={self.country_name}, season_year={self.season_year}, league_name={self.league_name},"
            f"round={self.round}, date={self.date}, status={self.status}, referee={self.referee},"
            f"home_team_id={self.home_team_id}, home_team_name={self.home_team_name},"
            f"away_team_id={self.away_team_id}, away_team_name={self.away_team_name},"
            f"goals_home={self.goals_home}, goals_away={self.goals_away})>"
            f"goals_home_ht={self.goals_home}, goals_away_ht={self.goals_away})>"
        )

    fixture_id = Column(Integer, primary_key=True)
    league_id = Column(Integer, ForeignKey("leagues.league_id"), nullable=False)
    season_id = Column(String, ForeignKey("seasons.season_id"), nullable=False)
    country_name = Column(String, nullable=False)
    season_year = Column(String, nullable=False)
    league_name = Column(String, nullable=False)
    round = Column(String)
    date = Column(Date)
    status = Column(String)
    referee = Column(String)
    home_team_id = Column(Integer, ForeignKey("teams.team_id"), nullable=False)
    home_team_name = Column(String, nullable=False)
    away_team_id = Column(Integer, ForeignKey("teams.team_id"), nullable=False)
    away_team_name = Column(String, nullable=False)
    goals_home = Column(Integer)
    goals_away = Column(Integer)
    goals_home_ht = Column(Integer)
    goals_away_ht = Column(Integer)
    # TODO: add 'if_break' column

    league = relationship("League", back_populates="fixture")
    season = relationship("Season", back_populates="fixture")
    # fixture_event = relationship("FixtureEvent", back_populates="fixture")
    # fixture_stat = relationship("FixtureStat", back_populates="fixture")
    # fixture_player_stat = relationship("FixturePlayerStat", back_populates="fixture")

    home_team = relationship(
        "Team", foreign_keys=[home_team_id], back_populates="home_team"
    )
    away_team = relationship(
        "Team", foreign_keys=[away_team_id], back_populates="away_team"
    )

    @classmethod
    def get_fixtures_dates_to_be_updated(cls) -> set:
        """
            Retrieve the set of fixture dates that need to be updated.

            This method queries the database to find the dates for fixtures
            that have not started yet for the specified season year (currently set
            to "2023"). It returns a set of date strings in the format 'YYYY-MM-DD'.

            Returns:
                set: A set of date strings representing the dates for fixtures
                    that need to be updated.

            Raises:
                Exception: If an error occurs during the database operation.
        """
        Session = sessionmaker(bind=get_engine())
        with Session() as session:
            try:
                curr_date = datetime.now().date()
                # Search min(date) for Not Started games
                dates_to_update = (
                    session.query(func.to_char(cls.date, 'YYYY-MM-DD'))
                    .filter((cls.date <= curr_date) & (cls.status == "NS") & (cls.season_year == "2023"))
                    .all()
                )
                dates_to_update_strings = [date_tuple[0] for date_tuple in dates_to_update]
                return set(dates_to_update_strings)
            except Exception as e:
                raise Exception

    @classmethod
    def update(cls, df: pd.DataFrame) -> None:
        Session = sessionmaker(bind=get_engine())
        with (Session() as session):
            try:
                # Convert DataFrame to list of dictionaries and update all rows into the database table using the session
                data = df.to_dict(orient="records")

                # Filter out rows for which fixture_id does not exist in the database
                fixture_ids_to_update = [row["fixture_id"] for row in data]
                existing_fixture_ids_in_db = [
                    row[0]
                    for row in session.query(cls.fixture_id)
                    .filter(cls.fixture_id.in_(fixture_ids_to_update))
                    .all()
                ]
                # data_to_update = df[df["fixture_id"].isin(existing_fixture_ids_in_db)]
                # TODO: delete if above works
                # [
                #     row for row in data if row["fixture_id"] in existing_fixture_ids_in_db
                # ]

                # Excluding newly pulled fixtures with team_ids that are not exist in Team table
                home_team_to_update = [row["home_team_id"] for row in data]
                away_team_to_update = [row["away_team_id"] for row in data]

                fixture_ids_without_invalid_teams = [
                    row[0]
                    for row in session.query(cls.fixture_id)
                    .filter(or_(cls.home_team_id.in_(home_team_to_update), cls.away_team_id.in_(away_team_to_update)))
                    .all()
                ]
                # tmp2_df = df[df['away_team_id'] == 23382]

                data_to_update = df[df["fixture_id"].isin(existing_fixture_ids_in_db) | df["fixture_id"].isin(fixture_ids_without_invalid_teams)]

                session.bulk_update_mappings(cls, data_to_update)
                session.commit()
                print(f"{cls.__name__} data updated successfully!")
            except Exception as e:
                # Rollback the session in case of an error to discard the changes
                session.rollback()
                print(f"Error while updating {cls.__name__} data: {e}")

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
        Session = sessionmaker(bind=get_engine())
        with Session() as session:
            try:
                max_date = (
                    session.query(func.max(cls.date)).filter(cls.status == "FT").scalar()
                )
                return max_date.strftime("%Y-%m-%d")
            except Exception as e:
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
        Session = sessionmaker(bind=get_engine())
        with Session() as session:
            try:
                today_fixtures_df = pd.read_sql_query(
                    session.query(cls).filter(cls.date == date.today()).statement, get_engine()
                )
                return today_fixtures_df
            except Exception as e:
                raise Exception

    @classmethod
    def get_season_fixtures_by_team(cls, team_id: int, season_year: str, status="ALL") -> pd.DataFrame:
        """
            Retrieve fixtures of a specific team for a given season and status.

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
        Session = sessionmaker(bind=get_engine())
        with Session() as session:
            try:
                team_fixtures_df = pd.read_sql_query(
                    session.query(cls)
                    .filter(
                        (cls.season_year == season_year)
                        & ((cls.home_team_id == team_id) | (cls.away_team_id == team_id))
                    )
                    .statement,
                    get_engine(),
                )
                # Return all except Not Started - most statuses are for finished games
                if status == "FT":
                    team_fixtures_df = team_fixtures_df[team_fixtures_df["status"] != "NS"]
                elif status == "NS":
                    team_fixtures_df = team_fixtures_df[team_fixtures_df["status"] == "NS"]
                return team_fixtures_df
            except Exception as e:
                raise Exception

    @staticmethod
    def filter_fixtures_by_rounds(df: pd.DataFrame, rounds):
        match rounds:
            case "all_finished":
                return df[df["fixture.status.short"] == "FT"]
            case "last_5":
                return df[df["fixture.status.short"] == "FT"].tail(5)
            case int():
                # If rounds is a number filter only "Regular Season" fixtures
                df = df[df["league.round"].str.contains("Regular Season")]
                return df[
                    df["league.round"].str.split("-").str[1].str.strip().astype(int)
                    <= rounds
                ]

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

        team_stats_grouped = (
            team_results_df.groupby(["team_group", "team_name"])
            .agg(
                games=("fixture_id", "count"),
                wins=("form", lambda x: (x == "W").sum()),
                draws=("form", lambda x: (x == "D").sum()),
                loses=("form", lambda x: (x == "L").sum()),
                goals_scored=("goals_home", "sum"),
                goals_conceded=("goals_away", "sum"),
                avg_goals_scored=("goals_home", lambda x: round(x.mean(), 2)),
                avg_goals_conceded=("goals_away", lambda x: round(x.mean(), 2)),
                form=("form", lambda x: "".join(x)),
            )
            .reset_index()
        )

        team_stats_total = (
            team_results_df.groupby("team_name")
            .agg(
                games=("fixture_id", "count"),
                wins=("form", lambda x: (x == "W").sum()),
                draws=("form", lambda x: (x == "D").sum()),
                loses=("form", lambda x: (x == "L").sum()),
                # goals_scored=(),
                # goals_conceded=(),
                # avg_goals_scored=(),
                # avg_goals_conceded=(),
                form=("form", lambda x: "".join(x)),
            )
            .reset_index()
        )

        # TODO: merge stats df

        return team_stats_grouped, team_stats_total

    @classmethod
    def create_game_preview(cls, home_team_id: int, away_team_id: int) -> pd.DataFrame:
        home_stats = cls.get_season_stats_by_team(home_team_id, "2023")[0]
        away_stats = cls.get_season_stats_by_team(away_team_id, "2023")[0]
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        game_preview_df = pd.concat([home_stats, away_stats])
        game_preview_df.to_csv(
            f"{SOURCE_DIR}/previews/{timestamp}_{home_team_id}-{away_team_id}.csv",
            index=False,
        )
        return game_preview_df

    @classmethod
    def get_overcome_games(cls) -> pd.DataFrame:
        overcome_mask = (
            (cls.goals_home_ht > cls.goals_away_ht) & (cls.goals_home < cls.goals_away)
        ) | (
            (cls.goals_home_ht < cls.goals_away_ht) & (cls.goals_home > cls.goals_away)
        )

        Session = sessionmaker(bind=get_engine())
        with Session() as session:
            try:
                overcome_games_df = pd.read_sql_query(
                    session.query(cls).filter(overcome_mask).statement, get_engine()
                )
                return overcome_games_df
            except Exception as e:
                # Rollback the session in case of an error to discard the changes
                session.rollback()
                print(f"Error while updating {cls.__name__} data: {e}")
