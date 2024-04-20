from sqlalchemy import (
    Column,
    Integer,
    ForeignKey,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from models.base import Base


class FixturePlayerStat(Base):
    __tablename__ = "fixtures_players_stats"

    # def __repr__(self):
    #     """Return a string representation of the FixtureStats object."""
    #     return (
    #         f"<FixtureEvent(fixture_id={self.fixture_id}, events={self.events})>"
    #     )

    fixture_id = Column(Integer, ForeignKey('fixtures.fixture_id'), primary_key=True)
    home_team_players_stats = Column(JSONB)
    away_team_players_stats = Column(JSONB)

    # fixture = relationship("Fixture", back_populates="fixtures_players_stats")
