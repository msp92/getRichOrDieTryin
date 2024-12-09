import logging
import pandas as pd
import typing


from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.schema import MetaData
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, scoped_session, Query
from typing import Callable
from typing_extensions import Optional, Any

from config.db_config import DbConfig


class Db:
    def __init__(self) -> None:
        self.config = DbConfig()
        self.engine = create_engine(self._construct_db_url(self.config))
        self.Session = scoped_session(sessionmaker(bind=self.engine))

    @staticmethod
    def _construct_db_url(config: DbConfig) -> str:
        """
        Constructs the database URL from a configuration dictionary.
        """
        user = config.DB_USER
        password = config.DB_PASSWORD
        host = config.DB_HOST
        port = config.DB_PORT
        database = config.DB_NAME

        return f"postgresql://{user}:{password}@{host}:{port}/{database}"

    def get_db_url(self) -> str:
        return self._construct_db_url(self.config)

    @contextmanager
    def get_session(self) -> typing.Generator[scoped_session, None, None]:
        session = self.Session
        try:
            yield session
        finally:
            session.close()

    def execute_raw_query(self, query: str | Query) -> Optional[pd.DataFrame]:
        """
        Executes a raw SQL query or SQLAlchemy query object.
        """
        try:
            with self.engine.connect() as connection:
                if isinstance(query, str):
                    return pd.read_sql_query(query, connection)
                else:
                    return pd.read_sql_query(query.statement, connection)
        except SQLAlchemyError as e:
            logging.error(f"Database query failed: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error occurred: {e}")
            raise

    def execute_orm_query(self, query: Callable[[scoped_session], Any]) -> typing.Any:
        """
        Executes an ORM query function that takes a session as an argument.
        """
        try:
            with self.get_session() as session:
                return query(session)
        except SQLAlchemyError as e:
            logging.error(f"Database query failed: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error occurred: {e}")
            raise

    def close(self) -> None:
        """
        Closes the engine and removes the session.
        """
        try:
            self.Session.remove()
            self.engine.dispose()
        except Exception as e:
            logging.error(f"Error during closing resources: {e}")

    def create_all_tables(self, metadata: MetaData) -> None:
        try:
            # Create all tables in the database
            logging.info("Creating all tables...")
            metadata.create_all(bind=self.engine)
        except Exception as e:
            # Handle any exceptions or errors that occur during the connection test
            logging.error(f"Error while creating tables: {e}")
            raise Exception

    def drop_all_tables(self, metadata: MetaData) -> None:
        try:
            if metadata.tables:
                logging.info("Dropping all tables...")
                metadata.drop_all(bind=self.engine)
        except Exception as e:
            # Handle any exceptions or errors that occur during the connection test
            logging.error(f"Error while dropping tables: {e}")
            raise Exception
