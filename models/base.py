import logging

from pandas import DataFrame
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
from sqlalchemy.orm import Mapper

from services.db import Db

db = Db()


class BaseMixin:
    __mapper__: Mapper = None
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
            except Exception as e:
                logging.error(f"Error while getting {cls.__name__} data: {str(e)}")
                raise Exception
            return df

    @classmethod
    def upsert(cls, df: pd.DataFrame) -> None:
        """
        Class method performing bulk upsert of provided DataFrame.
        """
        with db.get_session() as session:
            try:
                # Convert DataFrame to list of dictionaries
                records = df.to_dict(orient="records")
                existing_ids = cls.get_existing_records(df)
                cls.bulk_insert(records, existing_ids)
                cls.bulk_update(records, existing_ids)
            except Exception as e:
                # Rollback the session in case of an error to discard the changes
                session.rollback()
                logging.error(f"Error while upserting {cls.__name__} data: {e}")

    @classmethod
    def get_existing_records(cls, df: DataFrame) -> DataFrame:
        primary_key = cls.__mapper__.primary_key[0].name

        # Get IDs of input DataFrame
        ids = df[primary_key].tolist()
        if ids:
            with db.get_session() as session:
                # For input DataFrame get existing records from Db
                existing_records = pd.read_sql_query(
                    session.query(cls)
                    .filter(cls.__mapper__.primary_key[0].in_(ids))
                    .statement,
                    db.engine,
                )
                return existing_records

    @classmethod
    def bulk_insert(cls, records: list[dict], existing_records: DataFrame) -> None:
        primary_key = cls.__mapper__.primary_key[0].name

        with db.get_session() as session:
            try:
                # Prepare new records for insertion
                new_records = [
                    record
                    for record in records
                    if record[primary_key] not in existing_records[primary_key].values
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
    def bulk_update(cls, records: list[dict], existing_records: DataFrame) -> None:
        primary_key = cls.__mapper__.primary_key[0].name

        with db.get_session() as session:
            try:
                # Prepare records for updating
                records_to_update = [
                    record
                    for record in records
                    if record[primary_key] in existing_records[primary_key].values
                    and not cls._is_same_record(
                        record,
                        existing_records.loc[
                            existing_records[primary_key] == record[primary_key]
                        ].squeeze(),
                    )
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

    @classmethod
    def _is_same_record(cls, input_record: dict, existing_record: pd.Series) -> bool:
        """Helper function to check if the input record is the same as the existing record."""
        # Compare each field, except the primary key
        existing_record = existing_record
        # FIXME: update doesn't work for Coaches (only insert)
        for key, value in input_record.items():
            if (
                key != cls.__mapper__.primary_key[0].name
                and value != existing_record[key]
            ):
                return False
        return True


# Create a declarative base
Base = declarative_base(cls=BaseMixin)
