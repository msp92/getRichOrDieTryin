import logging

import pandas as pd
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from models.base import Base
from models.data.main import Country

# Specify the schema
SCHEMA_NAME = "dw_main"


class Team(Base):
    __tablename__ = "teams"
    __table_args__ = {"schema": SCHEMA_NAME}

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

        def choose_duplicates(group) -> None:
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
        Team.upsert(final_df)

        def choose_duplicates(group):
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
        Team.upsert(final_df)
