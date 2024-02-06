from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from models.base import Base


class Venue(Base):
    __tablename__ = 'venues'

    # country = relationship("Country", back_populates="venue")

    def __repr__(self):
        """Return a string representation of the User object."""
        return f"<Venue(id={self.venue_id}, country_id={self.country_id}, city={self.city}, name={self.name})>"

    venue_id = Column(Integer, primary_key=True)
    country_id = Column(Integer, ForeignKey('countries.country_id'), nullable=False)  # TODO: try to index FKs
    city = Column(String)
    name = Column(String)
