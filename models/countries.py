from sqlalchemy import Column, Integer, String, Sequence
from sqlalchemy.orm import relationship

from models.base import Base


class Country(Base):
    __tablename__ = "countries"

    league = relationship("League", back_populates="country")
    season = relationship("Season", back_populates="country")
    team = relationship("Team", back_populates="country")

    def __repr__(self):
        """Return a string representation of the Country object."""
        return (
            f"<Country(country_id={self.country_id}, country_name={self.country_name}, code={self.code},"
            f"flag={self.flag})>"
        )

    country_id = Column(Integer, Sequence("country_id_seq", 1), primary_key=True)
    country_name = Column(String, unique=True)
    code = Column(String)
    flag = Column(String)
