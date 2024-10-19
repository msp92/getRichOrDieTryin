from sqlalchemy import Column, Integer, String, Sequence
from models.base import Base


class Referee(Base):
    __tablename__ = "referees"

    referee_id = Column(Integer, Sequence("referee_id_seq", 1), primary_key=True)
    referee_name = Column(String, nullable=False)
