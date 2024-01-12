from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


# Create a base class
Base = declarative_base()


class League(Base):
    __table_name__ = 'leagues'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    type = Column(String)
    logo = Column(String)
    country_id = Column(Integer, ForeignKey('countries.id'), nullable=False)

    country = relationship("Country")


# Create tables in the database
Base.metadata.create_all(engine)


# Create a new user within a transaction
def create_user(session, name, email):
    new_user = User(name=name, email=email)
    session.add(new_user)
    session.commit()


# Use a session to manage transactions
session = Session()

try:
    # Start a transaction
    session.begin()

    # Create the first user
    create_user(session, "John Doe", "john.doe@example.com")

    # Create the second user
    create_user(session, "Jane Smith", "jane.smith@example.com")

    # Commit the transaction
    session.commit()
    print("Transaction committed successfully!")

except Exception as e:
    # Rollback the transaction if an error occurs
    session.rollback()
    print(f"Transaction rolled back due to error: {e}")

finally:
    # Close the session
    session.close()
