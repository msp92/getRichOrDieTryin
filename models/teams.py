from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from models.base import Base


class Team(Base):
    __tablename__ = "teams"

    country = relationship("Country", back_populates="team")
    home_team = relationship("Fixture", foreign_keys="Fixture.home_team_id")
    away_team = relationship("Fixture", foreign_keys="Fixture.away_team_id")

    def __repr__(self):
        """Return a string representation of the User object."""
        return (
            f"<Team(team_id={self.team_id}, country_id={self.country_id}, country_name={self.country_name},"
            f"team_name={self.team_name}, logo={self.logo})>"
        )

    team_id = Column(Integer, primary_key=True)
    country_id = Column(
        Integer, ForeignKey("countries.country_id"), nullable=False
    )  # TODO: try to index FKs
    country_name = Column(String, nullable=False)
    team_name = Column(String)
    logo = Column(String)
