import logging

from pandas import DataFrame
from sqlalchemy import or_, and_, inspect
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
from sqlalchemy.orm import Mapper

from helpers.utils import safe_str_to_int_cast
from services.db import Db

db = Db()


class BaseMixin:
    __mapper__: Mapper = None
    metadata = None
    __table__ = None

    @classmethod
    def get_primary_keys(cls) -> list[str]:
        return [key.name for key in cls.__mapper__.primary_key]

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
        primary_keys = cls.get_primary_keys()
        with db.get_session() as session:
            try:
                logging.info(f"Upserting {cls.__name__} data...")
                # Convert DataFrame to list of dictionaries
                records = df.to_dict(orient="records")
                existing_ids = cls.get_existing_records(df, primary_keys)
                cls.bulk_insert(records, existing_ids, primary_keys)
                cls.bulk_update(records, existing_ids, primary_keys)
                logging.info(f"{cls.__name__} data upserted successfully...")
            except Exception as e:
                # Rollback the session in case of an error to discard the changes
                session.rollback()
                logging.error(f"Error while upserting {cls.__name__} data: {e}")

    @classmethod
    def get_existing_records(cls, df: DataFrame, primary_keys: list[str]) -> DataFrame:
        try:
            # Get IDs of input DataFrame
            key_values = df[primary_keys].to_dict(orient="records")
        except KeyError as e:
            if "country_id" in e.args[0]:
                logging.error(
                    "Field 'country_id' of type Sequence is not available in the file and could't be checked against Df from input json file."
                )
            raise

        if key_values:
            with db.get_session() as session:
                # Build filter conditions for each key set
                conditions = [
                    and_(
                        *[getattr(cls, key) == value for key, value in key_set.items()]
                    )
                    for key_set in key_values
                ]
                # Query the database for matching records
                existing_records = pd.read_sql_query(
                    session.query(cls).filter(or_(*conditions)).statement,
                    db.engine,
                )
                return existing_records

    @classmethod
    def bulk_insert(
        cls,
        records: list[dict],
        existing_records: pd.DataFrame,
        primary_keys: list[str],
    ) -> None:
        if not existing_records.empty:
            # Convert primary key values in `existing_key_tuples` with safe casting
            existing_key_tuples = {
                tuple(
                    safe_str_to_int_cast(existing_records[key].iloc[i])
                    for key in primary_keys
                )
                for i in range(len(existing_records))
            }

            # Convert primary key values in `new_records` with safe casting
            new_records = [
                record
                for record in records
                if tuple(safe_str_to_int_cast(record[key]) for key in primary_keys)
                not in existing_key_tuples
            ]
        else:
            new_records = records

        with db.get_session() as session:
            try:
                logging.info(f"Inserting new {cls.__name__} records")
                # Get schema from __table_args__
                if isinstance(cls.__table_args__, dict):
                    schema = cls.__table_args__.get("schema", "public")
                elif isinstance(cls.__table_args__, tuple):
                    schema = next(
                        (
                            item.get("schema", "public")
                            for item in cls.__table_args__
                            if isinstance(item, dict)
                        ),
                        "public",
                    )
                else:
                    schema = "public"

                # Create table if it doesn't exist
                if not inspect(db.engine).has_table(cls.__tablename__, schema=schema):
                    cls.__table__.create(db.engine)

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
    def bulk_update(
        cls, records: list[dict], existing_records: pd.DataFrame, primary_keys: list
    ) -> None:
        if not existing_records.empty:
            # Convert primary key values in `existing_key_tuples` with safe casting
            existing_key_tuples = {
                tuple(
                    safe_str_to_int_cast(existing_records[key].iloc[i])
                    for key in primary_keys
                )
                for i in range(len(existing_records))
            }

            # Convert primary key values in `new_records` with safe casting
            records_to_update = [
                record
                for record in records
                if tuple(safe_str_to_int_cast(record[key]) for key in primary_keys)
                in existing_key_tuples
            ]
            # TODO: consider checking all fields values to avoid updating full tables
            if records_to_update:
                with db.get_session() as session:
                    try:
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
        primary_keys = [key.name for key in cls.__mapper__.primary_key]
        # FIXME: update doesn't work for Coaches (only insert)
        for key, value in input_record.items():
            if (
                key != primary_key and value != existing_record[key]
                for primary_key in primary_keys
            ):
                logging.info(f"Key {key}, value {value}")
                return False
        return True


# Create a declarative base
Base = declarative_base(cls=BaseMixin)
