from sqlalchemy import Column, Integer, String, Sequence, Date, Boolean, Dictionary
from sqlalchemy.ext.declarative import declarative_base


# Create a base class
Base = declarative_base()


class Season(Base):
    __table_name__ = 'seasons'

    id = Column(Integer, Sequence("id", 1), primary_key=True)
    league_id = Column(String, foreignKey="League.id", nullable=False)
    country_code = Column(String, foreignKey="Country.code", nullable=False)
    year = Column(Integer)
    start_date = Column(Date)
    end_date = Column(Date)
    current = Column(Boolean)
    coverage = Column(Dictionary)
