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
Session = sessionmaker(bind=engine)


def get_db_session():
    session = Session()
    return session


def check_connection():
    session = get_db_session()
    try:
        # Execute a simple query to fetch the PostgreSQL version
        version = session.execute(text("SELECT version();")).scalar()
        # Display the PostgreSQL version
        print(f"Connected to PostgreSQL version: {version}")
        return True
    except Exception as e:
        # Handle any exceptions or errors that occur during the connection test
        print(f"Connection Error: {e}")
        raise Exception
    finally:
        session.close()


def create_all_tables():
    try:
        # Create all tables in the database
        print("Creating all tables...")
        base_model.Base.metadata.create_all(bind=engine)
    except Exception as e:
        # Handle any exceptions or errors that occur during the connection test
        print(f"Error while creating tables: {e}")
        raise Exception


def drop_all_tables():
    try:
        if base_model.Base.metadata.tables:
            print("Dropping all tables...")
            base_model.Base.metadata.drop_all(bind=engine)
    except Exception as e:
        # Handle any exceptions or errors that occur during the connection test
        print(f"Error while dropping tables: {e}")
        raise Exception
