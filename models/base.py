import logging

from pandas import DataFrame
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd

from services.db import Db
from utils.vars import CURRENT_UTC_DATETIME

db = Db()


class BaseMixin:
    __mapper__ = None
    metadata = None
    __table__ = None

    @classmethod
    def get_df_from_table(cls) -> pd.DataFrame:
        """
        Retrieve all data from the database table associated with the class as a DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing all the data from the database table.

        Raises:
            Exception: If there's any error during the database query or processing.
        """
        with db.get_session() as session:
            try:
                df = pd.read_sql_query(session.query(cls).statement, db.engine)
                return df
            except Exception as e:
                logging.error(f"Error while getting {cls.__name__} data: {str(e)}")
                raise Exception

    @classmethod
    def upsert(cls, df: pd.DataFrame) -> None:
        """
        Class method to perform bulk upsert using a Pandas DataFrame.
        `cls` refers to the class, and `df` is a DataFrame with records to upsert.
        """
        with db.get_session() as session:
            try:
                # Convert DataFrame to list of dictionaries
                records = df.to_dict(orient="records")
                existing_ids = cls.get_existing_ids(df)
                cls.bulk_insert(records, existing_ids)
                cls.bulk_update(records, existing_ids)
            except Exception as e:
                # Rollback the session in case of an error to discard the changes
                session.rollback()
                logging.error(f"Error while upserting {cls.__name__} data: {e}")

    @classmethod
    def get_existing_ids(cls, df: DataFrame) -> list[str]:
        primary_key = cls.__mapper__.primary_key[0].name
        logging.debug(primary_key)

        # Separate records into those needing insert and those needing update
        ids = df[primary_key].tolist()
        if ids:
            with db.get_session() as session:
                existing_records = (
                    session.query(cls)
                    .filter(cls.__mapper__.primary_key[0].in_(ids))
                    .all()
                )
            return [getattr(record, primary_key) for record in existing_records]

    @classmethod
    def bulk_insert(cls, records: list[dict], existing_ids: list[str]) -> None:
        primary_key = cls.__mapper__.primary_key[0].name

        with db.get_session() as session:
            try:
                # Prepare new records for insertion
                new_records = [
                    {
                        **record,
                        "update_date": CURRENT_UTC_DATETIME,
                    }  # Add the new key:value pair
                    for record in records
                    if record[primary_key] not in existing_ids
                ]
                if new_records:
                    logging.info(f"Inserting new {cls.__name__} records")
                    session.bulk_insert_mappings(
                        cls, new_records
                    )  # Bulk insert new records
                    session.commit()
                    logging.info(
                        f"{len(new_records)} {cls.__name__} records inserted successfully"
                    )
            except Exception as e:
                # Rollback the session in case of an error to discard the changes
                session.rollback()
                logging.error(f"Error while inserting {cls.__name__} data: {e}")

    @classmethod
    def bulk_update(cls, records: list[dict], existing_ids: list[str]) -> None:
        primary_key = cls.__mapper__.primary_key[0].name

        with db.get_session() as session:
            try:
                # Prepare records for updating
                records_to_update = [
                    record for record in records if record[primary_key] in existing_ids
                ]
                if records_to_update:
                    logging.info(f"Updating {cls.__name__} records")
                    session.bulk_update_mappings(
                        cls, records_to_update
                    )  # Bulk update existing records
                    session.commit()
                    logging.info(
                        f"{len(records_to_update)} {cls.__name__} records updated successfully"
                    )
            except Exception as e:
                # Rollback the session in case of an error to discard the changes
                session.rollback()
                logging.error(f"Error while updating {cls.__name__} data: {e}")


# Create a declarative base
Base = declarative_base(cls=BaseMixin)
