from sqlalchemy import Column, Integer, String, Sequence, ForeignKey
from sqlalchemy.orm import relationship

from models.base import Base


class Country(Base):
    __tablename__ = 'countries'

    league = relationship("League")
    season = relationship("Season")
    #team = relationship("Team")
    #venue = relationship("Venue")

    def __repr__(self):
        """Return a string representation of the Country object."""
        return f"<Country(country_id={self.country_id}, name={self.name}, code={self.code}, flag={self.flag})>"

    country_id = Column(Integer, Sequence("country_id_seq", 1), primary_key=True)
    name = Column(String)
    code = Column(String)
    flag = Column(String)
