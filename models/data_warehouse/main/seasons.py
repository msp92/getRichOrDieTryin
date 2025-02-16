from sqlalchemy import Column, Integer, String, Date, Boolean, ForeignKey, JSON
from sqlalchemy.orm import relationship

from config.entity_names import SEASONS_TABLE_NAME, DW_MAIN_SCHEMA_NAME
from models.base import Base


class Season(Base):
    __tablename__ = SEASONS_TABLE_NAME
    __table_args__ = {"schema": DW_MAIN_SCHEMA_NAME}

    league = relationship("League", back_populates="season")
    country = relationship("Country", back_populates="season")

    season_id = Column(String, primary_key=True)
    league_id = Column(Integer, ForeignKey("dw_main.leagues.league_id"), nullable=False)
    country_id = Column(Integer, ForeignKey("dw_main.countries.country_id"), nullable=False)
    year = Column(Integer)
    start_date = Column(Date)
    end_date = Column(Date)
    current = Column(Boolean)
    coverage = Column(JSON)
