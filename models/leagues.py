from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from models.base import Base


class League(Base):
    __tablename__ = 'leagues'

    country = relationship("Country", back_populates="league")
    season = relationship("Season", back_populates="league")
    fixture = relationship("Fixture", back_populates="league")

    def __repr__(self):
        """Return a string representation of the League object."""
        return (f"<League(league_id={self.league_id}, country_id={self.country_id}, country_name={self.country_name},"
                f"name={self.name}, type={self.type}, logo={self.logo})>")

    league_id = Column(Integer, primary_key=True)
    country_id = Column(Integer, ForeignKey('countries.country_id'), nullable=False)
    country_name = Column(String, nullable=False)
    name = Column(String)
    type = Column(String)
    logo = Column(String)
