from sqlalchemy import create_engine, text, MetaData
from sqlalchemy.orm import sessionmaker
from config.config import DB_URL
from models.base import Base


def get_db_connection():
    # Create the engine
    engine = create_engine(DB_URL)
    # Create all tables in the database
    Base.metadata.create_all(engine)
    # Create a session factory - WHAT'S THE REASON???
    Session = sessionmaker(bind=engine)
    # Create a session
    session = Session()
    return session


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
    # Create the engine
    engine = create_engine(DB_URL)
    # Drop tables with relationships
    Base.metadata.drop_all(bind=engine, checkfirst=True)
