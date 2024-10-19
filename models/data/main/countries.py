from sqlalchemy import Column, Integer, String, Sequence
from sqlalchemy.orm import relationship
from models.base import Base

# Specify the schema
SCHEMA_NAME = "dw_main"


class Country(Base):
    __tablename__ = "countries"
    __table_args__ = {"schema": SCHEMA_NAME}

    league = relationship("League", back_populates="country")
    season = relationship("Season", back_populates="country")
    team = relationship("Team", back_populates="country")

    country_id = Column(Integer, Sequence("country_id_seq", 1), primary_key=True)
    country_name = Column(String, unique=True)
    code = Column(String)
    flag = Column(String)
