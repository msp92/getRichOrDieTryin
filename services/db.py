import logging

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from config.config import DB_URL
import models.base as base_model

### PostgreSQL commands ###
# DROP SCHEMA public CASCADE; - removes all tables & relations from database
# \l list databases
# \d list tables


# Create the engine
engine = create_engine(DB_URL)


def get_engine():
    return engine


def check_db_connection():
    Session = sessionmaker(bind=get_engine())
    with Session() as session:
        try:
            # Execute a simple query to fetch the PostgreSQL version
            version = session.execute(text("SELECT version();")).scalar()
            # Display the PostgreSQL version
            logging.info(f"Connected to PostgreSQL version: {version}")
            return True
        except Exception as e:
            # Handle any exceptions or errors that occur during the connection test
            logging.error(f"Connection Error: {e}")
            raise Exception


def create_all_tables():
    try:
        # Create all tables in the database
        logging.info("Creating all tables...")
        base_model.Base.metadata.create_all(bind=engine)
    except Exception as e:
        # Handle any exceptions or errors that occur during the connection test
        logging.error(f"Error while creating tables: {e}")
        raise Exception


def drop_all_tables():
    try:
        if base_model.Base.metadata.tables:
            logging.info("Dropping all tables...")
            base_model.Base.metadata.drop_all(bind=engine)
    except Exception as e:
        # Handle any exceptions or errors that occur during the connection test
        logging.error(f"Error while dropping tables: {e}")
        raise Exception
