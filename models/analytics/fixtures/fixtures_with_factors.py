from sqlalchemy import Column, Integer, String, DateTime, Numeric

from config.entity_names import ANALYTICS_FIXTURES_SCHEMA_NAME
from models.data.fixtures import Fixture



class FixturesWithFactors(Fixture):
    __tablename__ = "fixtures_with_factors"
    __table_args__ = {"schema": ANALYTICS_FIXTURES_SCHEMA_NAME}
    __mapper_args__ = {"concrete": True}

    fixture_id = Column(Integer, primary_key=True)
    date = Column(DateTime, nullable=False)
    home_team_name = Column(String, nullable=False)
    away_team_name = Column(String, nullable=False)
    goals_home = Column(Integer)
    goals_away = Column(Integer)
    goals_home_ht = Column(Integer)
    goals_away_ht = Column(Integer)
    home_team_factor = Column(Numeric(4, 2))
    away_team_factor = Column(Numeric(4, 2))
    total_factor = Column(Numeric(4, 2))
    home_team_breaks = Column(Integer)
    away_team_breaks = Column(Integer)
