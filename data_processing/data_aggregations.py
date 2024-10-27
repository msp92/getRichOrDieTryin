import logging

import pandas as pd

from config.vars import SOURCE_DIR
from models.data.fixtures import Fixture
from models.data.main import Team
from services.db import Db

db = Db()


def aggregate_breaks_team_stats_from_raw(df: pd.DataFrame) -> pd.DataFrame:
    # Initialize the dataframe with team_id and team_name
    breaks_team_stats_df = df.drop_duplicates("team_id")[
        ["team_id", "team_name"]
    ].reset_index(drop=True)

    df["date"] = pd.to_datetime(df[["year", "month", "day"]])
    last_break = df.groupby("team_id")["date"].max().reset_index()
    total = df.groupby("team_id").size().reset_index(name="total")
    home = (
        df.loc[df["side"] == "home"].groupby("team_id").size().reset_index(name="home")
    )
    away = (
        df.loc[df["side"] == "away"].groupby("team_id").size().reset_index(name="away")
    )

    # Merge counts into the dataframe
    breaks_team_stats_df = breaks_team_stats_df.merge(
        last_break, on="team_id", how="left"
    ).rename(columns={"date": "last_break"})
    breaks_team_stats_df = breaks_team_stats_df.merge(total, on="team_id", how="left")
    breaks_team_stats_df = breaks_team_stats_df.merge(home, on="team_id", how="left")
    breaks_team_stats_df = breaks_team_stats_df.merge(away, on="team_id", how="left")

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
    for month, month_name in months.items():
        month_count = (
            df.loc[df["month"] == month]
            .groupby("team_id")
            .size()
            .reset_index(name=month_name)
        )
        breaks_team_stats_df = breaks_team_stats_df.merge(
            month_count, on="team_id", how="left"
        )

    years = [
        2020,
        2021,
        2022,
        2023,
        2024,
    ]
    for year in years:
        year_count = (
            df.loc[df["year"] == year]
            .groupby("team_id")
            .size()
            .reset_index(name=f"c_{year}")
        )
        breaks_team_stats_df = breaks_team_stats_df.merge(
            year_count, on="team_id", how="left"
        )

    # Add counts for specific day ranges within a month
    beg_month = (
        df.loc[df["day"] < 11].groupby("team_id").size().reset_index(name="beg_month")
    )
    mid_month = (
        df.loc[(df["day"] > 11) & (df["day"] < 21)]
        .groupby("team_id")
        .size()
        .reset_index(name="mid_month")
    )
    end_month = (
        df.loc[df["day"] > 20].groupby("team_id").size().reset_index(name="end_month")
    )
    breaks_team_stats_df = breaks_team_stats_df.merge(
        beg_month, on="team_id", how="left"
    )
    breaks_team_stats_df = breaks_team_stats_df.merge(
        mid_month, on="team_id", how="left"
    )
    breaks_team_stats_df = breaks_team_stats_df.merge(
        end_month, on="team_id", how="left"
    )

    # TODO: convert round columns to int if possible
    for round_num in range(1, 61):
        round_count = (
            df.loc[df["round"] == str(round_num)]
            .groupby("team_id")
            .size()
            .reset_index(name=f"round_{round_num}")
        )
        breaks_team_stats_df = breaks_team_stats_df.merge(
            round_count, on="team_id", how="left"
        )

    # Add grouped round counts
    rounds_1_13 = (
        df.loc[df["round"].isin([str(i) for i in range(1, 14)])]
        .groupby("team_id")
        .size()
        .reset_index(name="rounds_1_13")
    )
    rounds_14_26 = (
        df.loc[df["round"].isin([str(i) for i in range(14, 27)])]
        .groupby("team_id")
        .size()
        .reset_index(name="rounds_14_26")
    )
    rounds_27_39 = (
        df.loc[df["round"].isin([str(i) for i in range(27, 40)])]
        .groupby("team_id")
        .size()
        .reset_index(name="rounds_27_39")
    )
    rounds_40_60 = (
        df.loc[df["round"].isin([str(i) for i in range(40, 61)])]
        .groupby("team_id")
        .size()
        .reset_index(name="rounds_40_60")
    )
    breaks_team_stats_df = breaks_team_stats_df.merge(
        rounds_1_13, on="team_id", how="left"
    )
    breaks_team_stats_df = breaks_team_stats_df.merge(
        rounds_14_26, on="team_id", how="left"
    )
    breaks_team_stats_df = breaks_team_stats_df.merge(
        rounds_27_39, on="team_id", how="left"
    )
    breaks_team_stats_df = breaks_team_stats_df.merge(
        rounds_40_60, on="team_id", how="left"
    )

    # Convert all columns except 'team_id' and 'team_name' to integers
    for col in breaks_team_stats_df.columns:
        if col not in ["team_id", "team_name", "last_break"]:
            breaks_team_stats_df[col] = breaks_team_stats_df[col].fillna(0).astype(int)

    return breaks_team_stats_df


def calculate_breaks_team_stats_shares_from_agg(df: pd.DataFrame) -> pd.DataFrame:
    columns_to_calculate = []
    shares_df = df[["team_id", "team_name", "last_break", "total"]].copy()

    for column in df.columns:
        if (
            column not in ["team_id", "team_name", "last_break", "total"]
            and "round_" not in column
        ):
            columns_to_calculate.append(column)

    for column in columns_to_calculate:
        shares_df[f"{column}_share"] = df[column] / df["total"]

    return shares_df


def calculate_no_draw_csv_for_all_teams() -> None:
    teams_df = Team.get_df_from_table()
    team_stats_df_list = []
    for idx, row in teams_df.iterrows():
        try:
            team_stats_grouped, team_stats_total = Fixture.get_season_stats_by_team(
                row["team_id"], "2023"
            )
            team_stats_total.filter(items=["team_name", "form"])
            team_stats_df_list.append(team_stats_total)
        except Exception as e:  # check what is the error and handle it
            logging.error(f"Error: {str(e)}")
            continue

    final_df = pd.concat(team_stats_df_list)
    final_df["no_draw"] = (
        final_df["form"].str[::-1].apply(lambda x: x.find("D") if "D" in x else -1)
    )
    final_df = final_df.filter(items=["team_name", "no_draw"])
    final_df = final_df[final_df["no_draw"] > 0]
    final_df.to_csv(
        f"{SOURCE_DIR}/team_stats_no_draw.csv",
        index=False,
    )


# Update table with single result
def update_table(table, home_team, away_team, home_goals, away_goals) -> pd.DataFrame:
    for team, goals, opponent_goals in [
        (home_team, home_goals, away_goals),
        (away_team, away_goals, home_goals),
    ]:

        if team not in table["Team"].values:
            table = pd.concat(
                [
                    table,
                    pd.DataFrame(
                        [
                            {
                                "Team": team,
                                "G": 0,
                                "W": 0,
                                "D": 0,
                                "L": 0,
                                "GF": 0,
                                "GA": 0,
                                "PTS": 0,
                            }
                        ]
                    ),
                ]
            )

        table.loc[table["Team"] == team, "G"] += 1
        table.loc[table["Team"] == team, "GF"] += goals
        table.loc[table["Team"] == team, "GA"] += opponent_goals

        if goals > opponent_goals:
            table.loc[table["Team"] == team, "PTS"] += 3
            table.loc[table["Team"] == team, "W"] += 1
        elif goals == opponent_goals:
            table.loc[table["Team"] == team, "PTS"] += 1
            table.loc[table["Team"] == team, "D"] += 1
        else:
            table.loc[table["Team"] == team, "L"] += 1
    return table


# Calculate table for input df
def calculate_table(league_id, season_year, rounds="all_finished"):
    """
    Create full table or as of round to calculate custom power factor
    """
    with db.get_session() as session:
        try:
            fixtures_df = pd.read_sql_query(
                session.query(Fixture)
                .filter(
                    (Fixture.league_id == league_id)
                    & (Fixture.season_year == season_year)
                    & (Fixture.status == "FT")
                )
                .statement,
                db.engine,
            )
        except Exception:
            raise Exception

    filtered_fixtures_df = Fixture.filter_fixtures_by_rounds(fixtures_df, rounds)

    table = pd.DataFrame(
        columns=[
            "Team",
            "G",
            "W",
            "D",
            "L",
            "GF",
            "GA",
            "PTS",
        ]
    )

    for idx, result in filtered_fixtures_df.iterrows():
        home_team = result["home_team_name"]
        away_team = result["away_team_name"]
        home_goals = int(result["goals_home"])
        away_goals = int(result["goals_away"])
        table = update_table(table, home_team, away_team, home_goals, away_goals)

    table = table.sort_values(by=["PTS", "GF"], ascending=[False, False]).reset_index(
        drop=True
    )
    table.index += 1
    return table
