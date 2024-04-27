from sqlalchemy import Column, Integer, String, Date, Sequence
from sqlalchemy.orm import relationship
from models.base import Base


class Pair(Base):
    __tablename__ = "pairs"

    def __repr__(self):
        """Return a string representation of the User object."""
        return (
            f"<Pair(pair_id={self.pair_id}, team_id_1={self.team_id_1}, team_name_1={self.team_name_1},"
            f"team_id_2={self.team_id_2}, team_name_2={self.team_name_2}, size={self.size},"
            f"first_game_date={self.first_game_date}, last_game_date={self.last_game_date})>"
        )

    pair_id = Column(Integer, Sequence("pair_id_seq", 1), primary_key=True)
    team_id_1 = Column(Integer, nullable=False)
    team_name_1 = Column(String, nullable=False)
    team_id_2 = Column(Integer, nullable=False)
    team_name_2 = Column(String, nullable=False)
    size = Column(Integer, nullable=False)
    first_game_date = Column(Date, nullable=False)
    last_game_date = Column(Date, nullable=False)
