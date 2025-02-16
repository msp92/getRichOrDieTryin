from sqlalchemy import Column, Integer, String, Date, PrimaryKeyConstraint

from config.entity_names import DW_MAIN_SCHEMA_NAME
from models.base import Base


class Coach(Base):
    __tablename__ = "coaches"
    __table_args__ = (
        PrimaryKeyConstraint(
            "coach_id", "team_id", "start_date", name="pk_coach_team_startDate"
        ),
        {"schema": DW_MAIN_SCHEMA_NAME},
    )

    coach_id = Column(Integer, nullable=False)
    coach_name = Column(String, nullable=False)
    age = Column(Integer, nullable=True)
    nationality = Column(String, nullable=True)
    team_id = Column(Integer)
    team_name = Column(String)
    start_date = Column(Date)
    end_date = Column(Date)
