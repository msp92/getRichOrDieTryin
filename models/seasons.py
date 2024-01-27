from sqlalchemy import Column, Integer, String, Sequence, Date, Boolean, ForeignKey, JSON
from sqlalchemy.orm import relationship
from models.base import Base


class Season(Base):
    __tablename__ = 'seasons'

    league = relationship("League", back_populates="season")
    country = relationship("Country", back_populates="season")
    # fixture = relationship("Fixture", back_populates="season")

    def __repr__(self):
        """Return a string representation of the User object."""
        return (f"<Season(season_id={self.season_id}, league_id={self.league_id}, country_id={self.country_id},"
                f"year={self.year}, start_date={self.start_date}, end_date={self.end_date},"
                f"current={self.current}, coverage={self.coverage})>")

    season_id = Column(String, primary_key=True)
    league_id = Column(Integer, ForeignKey('leagues.league_id'))  # TODO: try to index FKs
    country_id = Column(Integer, ForeignKey('countries.country_id'))  # TODO: try to index FKs
    year = Column(Integer)
    start_date = Column(Date)
    end_date = Column(Date)
    current = Column(Boolean)
    coverage = Column(JSON)
