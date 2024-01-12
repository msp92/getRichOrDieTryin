from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base


# Create a base class
Base = declarative_base()


class Country(Base):
    __table_name__ = 'countries'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    code = Column(String)
    flag = Column(String)
