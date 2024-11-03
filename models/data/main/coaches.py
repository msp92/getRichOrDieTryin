from sqlalchemy import Column, Integer, String, Date, PrimaryKeyConstraint
from models.base import Base

# Specify the schema
SCHEMA_NAME = "dw_main"


class Coach(Base):
    __tablename__ = "coaches"
    __table_args__ = (
        PrimaryKeyConstraint(
            "coach_id", "team_id", "start_date", name="pk_coach_team_startDate"
        ),
        {"schema": SCHEMA_NAME},
    )

    coach_id = Column(Integer, nullable=False)
    coach_name = Column(String, nullable=False)
    age = Column(Integer, nullable=True)
    nationality = Column(String, nullable=True)
    team_id = Column(Integer)
    team_name = Column(String)
    start_date = Column(Date)
    end_date = Column(Date)
