from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from models.base import Base

# Specify the schema
SCHEMA_NAME = "dw_main"


class Team(Base):
    __tablename__ = "teams"
    __table_args__ = {"schema": SCHEMA_NAME}

    country = relationship("Country", back_populates="team")
    home_team = relationship(
        "Fixture", foreign_keys="Fixture.home_team_id", back_populates="home_team"
    )
    away_team = relationship(
        "Fixture", foreign_keys="Fixture.away_team_id", back_populates="away_team"
    )

    team_id = Column(Integer, primary_key=True)
    country_id = Column(
        Integer, ForeignKey("dw_main.countries.country_id"), nullable=False
    )
    country_name = Column(String, nullable=False)
    team_name = Column(String)
    logo = Column(String)
