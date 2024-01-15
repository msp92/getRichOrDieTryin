from sqlalchemy import Column, Integer, String, Sequence
from sqlalchemy.orm import relationship

from models.base import Base


class Country(Base):
    __tablename__ = 'countries'

    league = relationship("League", back_populates="country")

    def __repr__(self):
        """Return a string representation of the Country object."""
        return f"<Country(id={self.id}, name={self.name}, code={self.code}, flag={self.flag})>"

    id = Column(Integer, Sequence("id", 1), primary_key=True)
    name = Column(String)
    code = Column(String)
    flag = Column(String)


