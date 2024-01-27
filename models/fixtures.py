from sqlalchemy import Column, Integer, String, Date, Sequence, ForeignKey
from sqlalchemy.orm import relationship

from models.base import Base


class Fixture(Base):
    __tablename__ = 'fixtures'

    league = relationship("League", back_populates="fixture")
    #season = relationship("Season")
    #venue = relationship("Venue", back_populates="fixture")
    #team = relationship("Team", back_populates="fixture")

    def __repr__(self):
        """Return a string representation of the User object."""
        return (f"<Fixture(fixture_id={self.fixture_id}, league_id={self.league_id}, date={self.date},"
                f"home_team_id={self.home_team_id}, away_team_id={self.away_team_id},"
                f"goals_home={self.goals_home}, goals_away={self.goals_away})>")

    fixture_id = Column(Integer, primary_key=True)
    league_id = Column(Integer, ForeignKey('leagues.league_id'), nullable=False)  # TODO: try to index FKs
    season_id = Column(String, nullable=False)  # TODO: try to index FKs
    # venue_id = Column(Integer, ForeignKey('venues.id'), nullable=False)  # TODO: try to index FKs
    date = Column(Date)
    home_team_id = Column(Integer, nullable=False)  # TODO: ForeignKey('teams.team_id') & try to index FKs
    away_team_id = Column(Integer, nullable=False)  # TODO: try to index FKs
    goals_home = Column(Integer)
    goals_away = Column(Integer)
    goals_home_ht = Column(Integer)
    goals_away_ht = Column(Integer)
