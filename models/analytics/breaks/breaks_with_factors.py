from sqlalchemy import Column, Integer, String, DateTime, Numeric

from models.data.fixtures.fixtures import Fixture
from services.db import Db

db = Db()

# Specify the schema
SCHEMA_NAME = "analytics_breaks"


class BreaksWithFactors(Fixture):
    __tablename__ = "breaks_with_factors"
    __table_args__ = {"schema": SCHEMA_NAME}
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
