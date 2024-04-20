from sqlalchemy import (
    Column,
    Integer,
    ForeignKey,
    String, Sequence,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from models.base import Base


class FixtureEvent(Base):
    __tablename__ = "fixtures_events"

    # def __repr__(self):
    #     """Return a string representation of the FixtureStats object."""
    #     return (
    #         f"<FixtureEvent(fixture_id={self.fixture_id}, events={self.events})>"
    #     )

    fixture_id = Column(Integer, ForeignKey('fixtures.fixture_id'), primary_key=True)
    events = Column(JSONB)

    # fixture = relationship("Fixture", back_populates="fixtures_events")
