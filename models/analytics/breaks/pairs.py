import logging

import pandas as pd
from numpy import product
from sqlalchemy import Column, Integer, String, Date, Sequence

from config.vars import DATA_DIR
from models.base import Base
from models.data.main import Team
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))


# Specify the schema
SCHEMA_NAME = "analytics_breaks"


class Pair(Base):
    __tablename__ = "pairs"
    __table_args__ = {"schema": SCHEMA_NAME}

    pair_id = Column(Integer, Sequence("pair_id_seq", 1), primary_key=True)
    team_id_1 = Column(Integer, nullable=False)
    team_name_1 = Column(String, nullable=False)
    team_id_2 = Column(Integer, nullable=False)
    team_name_2 = Column(String, nullable=False)
    size = Column(Integer, nullable=False)
    first_game_date = Column(Date, nullable=False)
    last_game_date = Column(Date, nullable=False)

    @staticmethod
    def search_coincidental_breaks_by_team_id(breaks_df: pd.DataFrame) -> None:
        unique_team_ids = pd.unique(
            pd.concat([breaks_df["home_team_id"], breaks_df["away_team_id"]])
        )
        unique_team_ids_df = pd.DataFrame({"team_id": unique_team_ids})
        teams_df = Team.get_df_from_table()

        unique_teams_df = pd.merge(
            unique_team_ids_df, teams_df, how="left", on="team_id"
        )

        # Iterate over the unique 'team_id's
        for i, first_team_row in unique_teams_df.iterrows():
            logging.info(
                f"[FIRST TEAM] Index: {i}/{len(unique_teams_df)}, team: {first_team_row['team_name']} ({first_team_row['country_name']})"
            )
            # Filter the overcome games for the current 'team_id'
            first_team_df = breaks_df[
                (breaks_df["home_team_id"] == first_team_row["team_id"])
                | (breaks_df["away_team_id"] == first_team_row["team_id"])
            ]
            # Check if the team has at least two games
            if len(first_team_df) < 5:
                continue
            # Prepare the data for calculations: convert pandas object => datetime => UNIX timestamp
            first_team_df = first_team_df.copy()  # Avoid SettingWithCopyWarning
            first_team_df.loc[:, "date_numeric"] = (
                pd.to_datetime(first_team_df["date"]).astype("int64") // 10**9
            )  # Convert nanoseconds to seconds

            logging.info("Searching for second team...")
            # Iterate over the overcome games of the current team
            for j, second_team_row in unique_teams_df.iterrows():
                if j > i:
                    second_team_df = breaks_df[
                        (breaks_df["home_team_id"] == second_team_row["team_id"])
                        | (breaks_df["away_team_id"] == second_team_row["team_id"])
                    ]
                    # Check if the team has at least two overcome games
                    if len(second_team_df) < 5:
                        continue
                    # Prepare the data for calculations: convert pandas object => datetime => UNIX timestamp
                    second_team_df = (
                        second_team_df.copy()
                    )  # Avoid SettingWithCopyWarning
                    second_team_df.loc[:, "date_numeric"] = (
                        pd.to_datetime(second_team_df["date"]).astype("int64") // 10**9
                    )  # Convert nanoseconds to seconds

                    # Prepare combinations of overcome games for both teams
                    combinations = list(
                        product(first_team_df.iterrows(), second_team_df.iterrows())
                    )
                    # Filter common overcome games for those within 5 days
                    result_combinations = [
                        pd.concat(
                            [
                                pd.DataFrame(
                                    [
                                        {
                                            "fixture_id": row_first["fixture_id"],
                                            "league_name": row_first["league_name"],
                                            "round": row_first["round"],
                                            "date": row_first["date"],
                                            "referee": row_first["referee"],
                                            "home_team_id": row_first["home_team_id"],
                                            "home_team_name": unique_teams_df.query(
                                                f"team_id == {row_first['home_team_id']}"
                                            ).iloc[0]["team_name"],
                                            "away_team_id": row_first["away_team_id"],
                                            "away_team_name": unique_teams_df.query(
                                                f"team_id == {row_first['away_team_id']}"
                                            ).iloc[0]["team_name"],
                                        }
                                    ]
                                ),
                                pd.DataFrame(
                                    [
                                        {
                                            "fixture_id": row_second["fixture_id"],
                                            "league_name": row_second["league_name"],
                                            "round": row_second["round"],
                                            "date": row_second["date"],
                                            "referee": row_second["referee"],
                                            "home_team_id": row_second["home_team_id"],
                                            "home_team_name": unique_teams_df.query(
                                                f"team_id == {row_second['home_team_id']}"
                                            ).iloc[0]["team_name"],
                                            "away_team_id": row_second["away_team_id"],
                                            "away_team_name": unique_teams_df.query(
                                                f"team_id == {row_second['away_team_id']}"
                                            ).iloc[0]["team_name"],
                                        }
                                    ]
                                ),
                            ]
                        )
                        for (_, row_first), (_, row_second) in combinations
                        if abs(row_first["date_numeric"] - row_second["date_numeric"])
                        <= 5 * 24 * 3600  # 5 days in seconds
                        and row_first["fixture_id"] != row_second["fixture_id"]
                    ]

                    comb_length = len(result_combinations)

                    if comb_length > 5:
                        # Write pair details into csv file
                        result_df = pd.concat(result_combinations).reset_index(
                            drop=True
                        )
                        # result_df.insert(0, "pair_id", pair_counter)
                        # pair_counter += 1
                        result_df.to_csv(
                            f"{DATA_DIR}/overcome_games/"
                            f"{comb_length}_{first_team_row['team_id']}&{second_team_row['team_id']}.csv",
                            index=False,
                        )
                        logging.info(
                            f"[SAVED] {comb_length}-COMBO FOR TEAMS: {first_team_row['team_name']} & {second_team_row['team_name']}"
                        )

                        # Insert general pair information
                        pair_df = pd.DataFrame(
                            {
                                "team_id_1": first_team_row["team_id"],
                                "team_name_1": first_team_row["team_name"],
                                "team_id_2": second_team_row["team_id"],
                                "team_name_2": second_team_row["team_name"],
                                "size": comb_length,
                                "first_game_date": min(result_df["date"]),
                                "last_game_date": max(result_df["date"]),
                            },
                            index=[0],
                        )

                        Pair.upsert(pair_df)
