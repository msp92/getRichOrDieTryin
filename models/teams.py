from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from models.base import Base


class Team(Base):
    __tablename__ = 'teams'

    country = relationship("Country", back_populates="team")
    fixture = relationship("Fixture")

    def __repr__(self):
        """Return a string representation of the User object."""
        return f"<Country(team_id={self.team_id}, country_id={self.country_id}, name={self.name}, logo={self.logo})>"

    team_id = Column(Integer, primary_key=True)
    country_id = Column(Integer, ForeignKey('countries.country_id'), nullable=False)  # TODO: try to index FKs
    name = Column(String)
    logo = Column(String)
