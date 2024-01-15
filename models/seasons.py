from sqlalchemy import Column, Integer, String, Sequence, Date, Boolean, ForeignKey, JSON
from sqlalchemy.orm import relationship
from models.base import Base


class Season(Base):
    __tablename__ = 'seasons'

    def __repr__(self):
        """Return a string representation of the User object."""
        return (f"<Season(id={self.id}, league_id={self.league_id}, country_id={self.country_id},"
                f"year={self.year}, start_date={self.start_date}, end_date={self.end_date},"
                f"current={self.current}, coverage={self.coverage})>")

    id = Column(Integer, Sequence("id", 1), primary_key=True)
    league_id = Column(Integer, ForeignKey('leagues.id'), nullable=False)
    country_id = Column(Integer, ForeignKey('countries.id'), nullable=False)
    year = Column(Integer)
    start_date = Column(Date)
    end_date = Column(Date)
    current = Column(Boolean)
    coverage = Column(JSON)

    league = relationship("League")
    country = relationship("Country")
