import logging
import typing

import pandas as pd
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship

from config.entity_names import TEAMS_TABLE_NAME, DW_MAIN_SCHEMA_NAME
from models.base import Base
from models.data.fixtures import Fixture
from models.data.main import Country


class Team(Base):
    __tablename__ = TEAMS_TABLE_NAME
    __table_args__ = {"schema": DW_MAIN_SCHEMA_NAME}

    country = relationship("Country", back_populates="team")
    # coach = relationship("Coach", foreign_keys="Coach.team_id", back_populates="team")
    home_team = relationship(
        "Fixture", foreign_keys="Fixture.home_team_id", back_populates="home_team"
    )
    away_team = relationship(
        "Fixture", foreign_keys="Fixture.away_team_id", back_populates="away_team"
    )

    team_id = Column(Integer, primary_key=True)
    country_id = Column(
        Integer, ForeignKey("dw_main.countries.country_id"), nullable=False
    )
    country_name = Column(String, nullable=False)
    team_name = Column(String)
    logo = Column(String)

    @staticmethod
    def insert_missing_teams_into_db(df: pd.DataFrame) -> None:
        logging.info("Checking for teams in fixtures that are missing in Team table...")
        # Get unique teams from all pulled fixtures
        unique_team_ids = pd.unique(pd.concat([df["home_team_id"], df["away_team_id"]]))
        unique_team_ids_df = pd.DataFrame({"team_id": unique_team_ids})

        # Mark which teams exists in Team table and which do not
        merged_df = pd.merge(
            unique_team_ids_df,
            Team.get_df_from_table(),
            on="team_id",
            how="left",
            indicator=True,
        )
        # Get only those who do not exist in Team table
        missing_ids_df = merged_df[merged_df["_merge"] == "left_only"].drop(
            columns=["_merge"]
        )
        logging.info(
            f"Number of missing teams from current fixtures: {len(missing_ids_df)}"
        )

        # Get fixtures of missing teams
        missing_teams_in_fixtures_df = df[
            df["home_team_id"].isin(missing_ids_df["team_id"])
            | df["away_team_id"].isin(missing_ids_df["team_id"])
        ]
        # Get filtered home/away dfs
        home_teams_missing = missing_teams_in_fixtures_df[
            ["home_team_id", "home_team_name", "league_id", "country_name"]
        ].rename(columns={"home_team_id": "team_id", "home_team_name": "team_name"})
        away_teams_missing = missing_teams_in_fixtures_df[
            ["away_team_id", "away_team_name", "league_id", "country_name"]
        ].rename(columns={"away_team_id": "team_id", "away_team_name": "team_name"})

        # Merge home/away dfs
        unique_teams_df = pd.concat(
            [home_teams_missing, away_teams_missing]
        ).drop_duplicates(subset=["team_id", "team_name", "country_name"])
        # Get only missing teams
        unique_teams_filtered_df = unique_teams_df[
            unique_teams_df["team_id"].isin(missing_ids_df["team_id"])
        ].sort_values(by="team_id", ascending=True)
        unique_teams_filtered_df["logo"] = ""

        # Get correct unique teams
        missing_teams_to_insert_df = unique_teams_filtered_df.drop_duplicates(
            subset="team_id", keep=False
        )
        # Get problematic duplicates
        teams_to_fix_df = unique_teams_filtered_df[
            unique_teams_filtered_df.duplicated(subset="team_id", keep=False)
        ]

        def choose_duplicates(group: pd.DataFrame) -> typing.Any:
            # Check if 'country_id' values are the same (case: different team_name for the same team)
            if group["team_name"].nunique() == 2:
                # Remove row with shorter 'team_name'
                shortest_name_index = group["team_name"].str.len().idxmax()
                return group.drop(index=shortest_name_index)

            # If 'country_id' values are different (case: different country for the same team)
            else:
                # Check if one row has country_id=166 (case: World & other country)
                if "World" in group["country_name"].values:
                    # Remove row with 'country_id'=166
                    index_to_remove = group[group["country_name"] == "World"].index
                    return group.drop(index=index_to_remove)
                else:
                    # Remove row with smaller country_id (case: two different countries - Aruba(8) & Netherlands(90))
                    min_country_id_index = group["country_name"].idxmin()
                    return group.drop(index=min_country_id_index)

        # Apply the custom function to handle duplicates
        deduplicated_teams_df = teams_to_fix_df.groupby("team_id").apply(
            choose_duplicates
        )

        # Merge both subsets
        concatenated_df = pd.concat([missing_teams_to_insert_df, deduplicated_teams_df])
        # Add country_id from League table
        final_df = pd.merge(
            concatenated_df, Country.get_df_from_table(), on="country_name", how="left"
        ).filter(items=["team_id", "country_id", "country_name", "team_name", "logo"])
        if not final_df.empty:
            Team.upsert(final_df)

    @staticmethod
    def get_season_stats_by_team(team_id: int, season_year: str) -> pd.DataFrame:
        team_results_df = Fixture.get_season_fixtures_by_team(
            team_id, season_year, "FT"
        )
        team_stats_df = Fixture.get_season_stats_by_team(team_id, season_year, "FT")

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
                # goals_scored=("goals_home" if "team_group" == "home" else "goals_away", "sum"),  # FIXME: didn't sum as expected
                # goals_conceded=("goals_away" if "team_group" == "home" else "goals_home", "sum"),  # FIXME: didn't sum as expected
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
