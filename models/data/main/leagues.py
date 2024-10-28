from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from models.base import Base

# Specify the schema
SCHEMA_NAME = "dw_main"


class League(Base):
    __tablename__ = "leagues"
    __table_args__ = {"schema": SCHEMA_NAME}

    league_id = Column(Integer, primary_key=True)
    country_id = Column(
        Integer, ForeignKey("dw_main.countries.country_id"), nullable=False
    )
    country_name = Column(String, nullable=False)
    league_name = Column(String)
    type = Column(String)
    logo = Column(String)

    country = relationship("Country", back_populates="league")
    season = relationship("Season", back_populates="league")
    fixture = relationship("Fixture", back_populates="league")
