from sqlalchemy import (
    Column,
    Integer,
    ForeignKey,
    PrimaryKeyConstraint,
    String,
)
from sqlalchemy.dialects.postgresql import JSONB
from models.base import Base

# Specify the schema
SCHEMA_NAME = "dw_fixtures"


class FixtureStat(Base):
    __tablename__ = "fixtures_stats"
    __table_args__ = (
        PrimaryKeyConstraint("fixture_id", "side", name="pk_fixture_side"),
        {"schema": SCHEMA_NAME},
    )

    fixture_id = Column(
        Integer, ForeignKey("dw_fixtures.fixtures.fixture_id"), nullable=False
    )
    side = Column(String)
    team_id = Column(Integer)
    team_name = Column(String)
    statistics = Column(JSONB)
