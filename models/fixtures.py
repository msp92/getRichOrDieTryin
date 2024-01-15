from sqlalchemy import Column, Integer, String, Date, Sequence
from models.base import Base


class Fixture(Base):
    __tablename__ = 'fixtures'

    def __repr__(self):
        """Return a string representation of the User object."""
        return f"<Fixture(id={self.id}, date={self.date}, league_id={self.league_id}, country_id={self.country_id})>"

    id = Column(Integer, Sequence("id", 1), primary_key=True)
    date = Column(Date)
    league_id = Column(String)
    country_id = Column(String)
