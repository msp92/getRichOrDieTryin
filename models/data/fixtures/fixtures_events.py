from sqlalchemy import (
    Column,
    Integer,
    ForeignKey,
    String,
    PrimaryKeyConstraint,
)
from models.base import Base

# Specify the schema
SCHEMA_NAME = "dw_fixtures"


class FixtureEvent(Base):
    __tablename__ = "fixtures_events"
    __table_args__ = (
        PrimaryKeyConstraint("fixture_id", "event_id", name="pk_fixtureId_eventId"),
        {"schema": SCHEMA_NAME},
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
