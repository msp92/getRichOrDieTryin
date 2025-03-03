import pandas as pd
from sqlalchemy import Column, Integer, String, Date, DateTime, asc
from sqlalchemy.exc import InvalidRequestError

from config.entity_names import ANALYTICS_BREAKS_SCHEMA_NAME
from models.base import Base


class BreaksTeamStats(Base):
    __tablename__ = "breaks_team_stats"
    __table_args__ = {"schema": ANALYTICS_BREAKS_SCHEMA_NAME}
    __mapper_args__ = {"concrete": True}

    team_id = Column(Integer, primary_key=True)
    team_name = Column(String, nullable=False)
    last_break = Column(Date, nullable=False)
    total = Column(Integer)
    home = Column(Integer)
    away = Column(Integer)
    won = Column(Integer)
    lost = Column(Integer)
    jan = Column(Integer)
    feb = Column(Integer)
    mar = Column(Integer)
    apr = Column(Integer)
    may = Column(Integer)
    jun = Column(Integer)
    jul = Column(Integer)
    aug = Column(Integer)
    sep = Column(Integer)
    oct = Column(Integer)
    nov = Column(Integer)
    dec = Column(Integer)
    beg_month = Column(Integer)
    mid_month = Column(Integer)
    end_month = Column(Integer)
    c_2020 = Column("2020", Integer)
    c_2021 = Column("2021", Integer)
    c_2022 = Column("2022", Integer)
    c_2023 = Column("2023", Integer)
    c_2024 = Column("2024", Integer)
    round_1 = Column("round_1", Integer)
    round_2 = Column("round_2", Integer)
    round_3 = Column("round_3", Integer)
    round_4 = Column("round_4", Integer)
    round_5 = Column("round_5", Integer)
    round_6 = Column("round_6", Integer)
    round_7 = Column("round_7", Integer)
    round_8 = Column("round_8", Integer)
    round_9 = Column("round_9", Integer)
    round_10 = Column("round_10", Integer)
    round_11 = Column("round_11", Integer)
    round_12 = Column("round_12", Integer)
    round_13 = Column("round_13", Integer)
    round_14 = Column("round_14", Integer)
    round_15 = Column("round_15", Integer)
    round_16 = Column("round_16", Integer)
    round_17 = Column("round_17", Integer)
    round_18 = Column("round_18", Integer)
    round_19 = Column("round_19", Integer)
    round_20 = Column("round_20", Integer)
    round_21 = Column("round_21", Integer)
    round_22 = Column("round_22", Integer)
    round_23 = Column("round_23", Integer)
    round_24 = Column("round_24", Integer)
    round_25 = Column("round_25", Integer)
    round_26 = Column("round_26", Integer)
    round_27 = Column("round_27", Integer)
    round_28 = Column("round_28", Integer)
    round_29 = Column("round_29", Integer)
    round_30 = Column("round_30", Integer)
    round_31 = Column("round_31", Integer)
    round_32 = Column("round_32", Integer)
    round_33 = Column("round_33", Integer)
    round_34 = Column("round_34", Integer)
    round_35 = Column("round_35", Integer)
    round_36 = Column("round_36", Integer)
    round_37 = Column("round_37", Integer)
    round_38 = Column("round_38", Integer)
    round_39 = Column("round_39", Integer)
    round_40 = Column("round_40", Integer)
    round_41 = Column("round_41", Integer)
    round_42 = Column("round_42", Integer)
    round_43 = Column("round_43", Integer)
    round_44 = Column("round_44", Integer)
    round_45 = Column("round_45", Integer)
    round_46 = Column("round_46", Integer)
    round_47 = Column("round_47", Integer)
    round_48 = Column("round_48", Integer)
    round_49 = Column("round_49", Integer)
    round_50 = Column("round_50", Integer)
    round_51 = Column("round_51", Integer)
    round_52 = Column("round_52", Integer)
    round_53 = Column("round_53", Integer)
    round_54 = Column("round_54", Integer)
    round_55 = Column("round_55", Integer)
    round_56 = Column("round_56", Integer)
    round_57 = Column("round_57", Integer)
    round_58 = Column("round_58", Integer)
    round_59 = Column("round_59", Integer)
    round_60 = Column("round_60", Integer)
    rounds_1_13 = Column("rounds_1-13", Integer)
    rounds_14_26 = Column("rounds_14-26", Integer)
    rounds_27_39 = Column("rounds_27-39", Integer)
    rounds_40_60 = Column("rounds_40-60", Integer)
    update_date = Column(DateTime)

    @classmethod
    def get_all(cls):
        with cls.db.get_session() as session:
            try:
                breaks_team_stats_df = pd.read_sql_query(
                    session.query(cls).order_by(asc(cls.team_id)).statement,
                    cls.db.engine,
                )
            except InvalidRequestError as e:
                raise InvalidRequestError(
                    f"Error while reading {cls.__name__} data: {e}"
                )
        return breaks_team_stats_df
