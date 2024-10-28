from sqlalchemy import (
    Column,
    Integer,
    ForeignKey,
)
from sqlalchemy.dialects.postgresql import JSONB
from models.base import Base

# Specify the schema
SCHEMA_NAME = "dw_fixtures"


class FixtureStat(Base):
    __tablename__ = "fixtures_stats"
    __table_args__ = {"schema": SCHEMA_NAME}

    fixture_id = Column(
        Integer, ForeignKey("dw_fixtures.fixtures.fixture_id"), primary_key=True
    )
    home_team_stats = Column(JSONB)
    away_team_stats = Column(JSONB)
