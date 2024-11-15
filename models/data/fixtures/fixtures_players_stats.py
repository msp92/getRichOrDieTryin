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


class FixturePlayerStat(Base):
    __tablename__ = "fixtures_players_stats"
    __table_args__ = (
        PrimaryKeyConstraint("fixture_id", "player_id", name="pk_fixture_player"),
        {"schema": SCHEMA_NAME},
    )

    fixture_id = Column(
        Integer, ForeignKey("dw_fixtures.fixtures.fixture_id"), nullable=False
    )
    side = Column(String)
    team_id = Column(Integer)
    team_name = Column(String)
    player_id = Column(Integer)
    player_name = Column(String)
    statistics = Column(JSONB)
