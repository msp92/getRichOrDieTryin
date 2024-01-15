from sqlalchemy import Column, Integer, String, ForeignKey, Sequence
from sqlalchemy.orm import relationship
from models.base import Base
from models.countries import Country


class League(Base):
    __tablename__ = 'leagues'

    country = relationship("Country", back_populates="league")

    def __repr__(self):
        """Return a string representation of the League object."""
        return (f"<League(id={self.id}, name={self.name}, type={self.type},"
                f"logo={self.logo}, country_id={self.country_id})>")

    id = Column(Integer, primary_key=True)
    country_id = Column(Integer, ForeignKey('countries.id'))  # , nullable=False
    name = Column(String)
    type = Column(String)
    logo = Column(String)




