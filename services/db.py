from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from config.config import DB_URL
from models.base import Base


### PostgreSQL commands ###
# DROP SCHEMA public CASCADE; - removes all tables & relations from database
# \l list databases
# \d list tables


# Create the engine
engine = create_engine(DB_URL)


def get_db_connection():
    return engine


def get_db_session():
    Session = sessionmaker(bind=engine)
    # Create a session
    session = Session()
    return session


def create_all_tables():
    # Create all tables in the database
    Base.metadata.create_all(bind=engine)


def check_connection(session):
    try:
        # Execute a simple query to fetch the PostgreSQL version
        version = session.execute(text('SELECT version();')).scalar()

        # Display the PostgreSQL version
        print(f"Connected to PostgreSQL version: {version}")
        return True

    except Exception as e:
        # Handle any exceptions or errors that occur during the connection test
        print(f"Connection Error: {e}")
        raise Exception


def drop_all_tables():
    print("Dropping all tables...")
    # Drop tables with relationships
    Base.metadata.drop_all(bind=engine, checkfirst=True)
