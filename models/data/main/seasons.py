from sqlalchemy import Column, Integer, String, Date, Boolean, ForeignKey, JSON
from sqlalchemy.orm import relationship
from models.base import Base

# Specify the schema
SCHEMA_NAME = "dw_main"


class Season(Base):
    __tablename__ = "seasons"
    __table_args__ = {"schema": SCHEMA_NAME}

    league = relationship("League", back_populates="season")
    country = relationship("Country", back_populates="season")

    season_id = Column(String, primary_key=True)
    league_id = Column(Integer, ForeignKey("dw_main.leagues.league_id"), nullable=False)
    country_id = Column(
        Integer, ForeignKey("dw_main.countries.country_id"), nullable=False
    )
    year = Column(Integer)
    start_date = Column(Date)
    end_date = Column(Date)
    current = Column(Boolean)
    coverage = Column(JSON)
