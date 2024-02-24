from sqlalchemy import func
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
from services.db import get_db_session, engine


class BaseMixin:
    __table__ = None

    @classmethod
    def get_columns_list(cls):
        return [column.name for column in cls.__table__.columns]

    @classmethod
    def get_all(cls):
        session = get_db_session()
        try:
            # Fetch all users
            query = session.query(cls).all()
            return query
        except Exception as e:
            print(f"Error while getting all records: {e}")
        finally:
            session.close()

    @classmethod
    def count_all(cls):
        session = get_db_session()
        try:
            # Fetch all users
            count_all = session.query(func.count()).select_from(cls).scalar()
            return count_all
        except Exception as e:
            print(f"Error while getting {cls.__name__} count: {e}")
        finally:
            session.close()

    @classmethod
    def get_df_from_table(cls) -> pd.DataFrame:
        session = get_db_session()
        try:
            # Query the data
            df = pd.read_sql_query(session.query(cls).statement, engine)
            return df
        except Exception as e:
            print(f"Error while getting {cls.__name__} data: {str(e)}")
        finally:
            session.close()

    @classmethod
    def get_id_by_name(cls, name: str):
        session = get_db_session()
        try:
            print(f"Id of {name}: {session.query(cls).filter_by(name=name).first()}")
            return session.query(cls).filter_by(name=name).first()
        except Exception as e:
            print(f"Error while getting id of {name}: {str(e)}")
        finally:
            session.close()

    @classmethod
    def insert_df(cls, df: pd.DataFrame):
        session = get_db_session()
        try:
            # Convert DataFrame to list of dictionaries and insert all rows into the database table using the session
            data = df.to_dict(orient="records")
            session.bulk_insert_mappings(cls, data)
            session.commit()
            print(f"{cls.__name__} data inserted successfully!")
        except Exception as e:
            # Rollback the session in case of an error to discard the changes
            session.rollback()
            print(f"Error while inserting {cls.__name__} data: {e}")
        finally:
            session.close()


# Create a declarative base
Base = declarative_base(cls=BaseMixin)
