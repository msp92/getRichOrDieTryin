from sqlalchemy import (
    Column,
    Integer,
    ForeignKey,
    String,
    PrimaryKeyConstraint,
)
import datetime as dt
from config.entity_names import DW_FIXTURES_SCHEMA_NAME
from models.base import Base


class FixtureEvent(Base):
    __tablename__ = "fixtures_events"
    __table_args__ = (
        PrimaryKeyConstraint("fixture_id", "event_id", name="pk_fixtureId_eventId"),
        {"schema": DW_FIXTURES_SCHEMA_NAME},
    )
    fixture_id = Column(
        Integer, ForeignKey("dw_fixtures.fixtures.fixture_id"), nullable=False
    )
    event_id = Column(Integer)
    elapsed_time = Column(Integer)
    extra_time = Column(Integer)
    event_type = Column(String)
    event_detail = Column(String)
    team_id = Column(Integer)
    team_name = Column(String)

    @staticmethod
    def get_dates_to_update() -> list[str]:
        curr_date = dt.date.today()
        start_date = curr_date - dt.timedelta(days=5)

        # Generate list of dates as strings
        date_range = [
            (start_date + dt.timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range((curr_date - start_date).days + 1)
        ]
        return date_range
