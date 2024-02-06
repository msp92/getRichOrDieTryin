from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
from services.db import get_db_session


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
            # Process query results
            for record in query:
                print(record)
        except Exception as e:
            print(f"Error while getting all records: {e}")
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
    def get_df_from_table(cls) -> pd.DataFrame:
        session = get_db_session()
        try:
            # Query the data
            data = session.query(cls).all()
            # Create a DataFrame from the query result
            columns = list(cls.__table__.columns.keys())
            df = pd.DataFrame(
                [{col: getattr(row, col) for col in columns} for row in data],
                columns=columns,
            )
            return df
        except Exception as e:
            print(f"Error while getting {cls.__name__} data: {str(e)}")
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
