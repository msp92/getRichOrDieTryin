from sqlalchemy import (
    Column,
    Integer,
    ForeignKey,
    PrimaryKeyConstraint,
    String,
)
from sqlalchemy.dialects.postgresql import JSONB
import datetime as dt

from sqlalchemy.orm import relationship

from config.entity_names import DW_FIXTURES_SCHEMA_NAME
from models.base import Base


class FixtureStat(Base):
    __tablename__ = "fixtures_stats"
    __table_args__ = (
        PrimaryKeyConstraint("fixture_id", "side", name="pk_fixture_side"),
        {"schema": DW_FIXTURES_SCHEMA_NAME},
    )

    fixture = relationship("Fixture", back_populates="stats")

    fixture_id = Column(
        Integer, ForeignKey("dw_fixtures.fixtures.fixture_id"), nullable=False
    )
    side = Column(String)
    team_id = Column(Integer)
    team_name = Column(String)
    statistics = Column(JSONB)

    @staticmethod
    def get_dates_to_update() -> list[str]:
        curr_date = dt.date.today()
        start_date = curr_date - dt.timedelta(days=2)

        # Generate list of dates as strings
        date_range = [
            (start_date + dt.timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range((curr_date - start_date).days + 1)
        ]
        return date_range
