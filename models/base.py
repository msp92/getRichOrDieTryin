import logging
from sqlalchemy import func, create_engine
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
from sqlalchemy.orm import sessionmaker

from config.config import DB_URL
from services.db import get_engine


class BaseMixin:
    __table__ = None

    @classmethod
    def get_columns_list(cls) -> list:
        """
            Retrieve a list of column names for the table associated with the class.

            Returns:
                list: A list of column names.
        """
        return [column.name for column in cls.__table__.columns]

    @classmethod
    def count_all(cls) -> int:
        """
            Count all records in the database table associated with the class.

            Returns:
                int: The total count of records.

            Raises:
                Exception: If there's any error during the database query or processing.
        """
        Session = sessionmaker(bind=get_engine())
        with Session() as session:
            try:
                # Fetch all users
                count_all = session.query(func.count()).select_from(cls).scalar()
                return count_all
            except Exception as e:
                logging.error(f"Error while getting {cls.__name__} count: {e}")
                raise Exception

    @classmethod
    def get_df_from_table(cls) -> pd.DataFrame:
        """
            Retrieve all data from the database table associated with the class as a DataFrame.

            Returns:
                pd.DataFrame: A DataFrame containing all the data from the database table.

            Raises:
                Exception: If there's any error during the database query or processing.
        """
        Session = sessionmaker(bind=get_engine())
        with Session() as session:
            try:
                df = pd.read_sql_query(session.query(cls).statement, get_engine())
                return df
            except Exception as e:
                logging.error(f"Error while getting {cls.__name__} data: {str(e)}")
                raise Exception

    @classmethod
    def insert_df(cls, df: pd.DataFrame) -> None:
        """
            Insert data from a DataFrame into the database table associated with the class.

            Args:
                df (pd.DataFrame): The DataFrame containing the data to be inserted.

            Raises:
                Exception: If there's any error during the database insertion or processing.
        """
        # FIXME: stop creating engine for each transaction and use connection pools using PgBouncer
        Session = sessionmaker(bind=create_engine(DB_URL))
        with Session() as session:
            try:
                # Convert DataFrame to list of dictionaries and insert all rows into db
                data = df.to_dict(orient="records")
                session.bulk_insert_mappings(cls, data)
                session.commit()
                logging.info(f"Successfully inserted {len(data)} records to {cls.__name__} table!")
            except Exception as e:
                # Rollback the session in case of an error to discard the changes
                session.rollback()
                logging.error(f"Error while inserting {cls.__name__} data: {e}")
                raise Exception


# Create a declarative base
Base = declarative_base(cls=BaseMixin)
