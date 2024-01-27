from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
import pandas as pd


class BaseMixin:
    __table__ = None

    @classmethod
    def get_all(cls, session: Session):
        try:
            # Fetch all users
            query = session.query(cls).all()
            # Process query results
            for record in query:
                print(record)
        except Exception as e:
            print(f"Error: {e}")
            return False

    @classmethod
    def get_id_by_name(cls, session: Session, name: str):
        print(f"Id of {name}: {session.query(cls).filter_by(name=name).first()}")
        return session.query(cls).filter_by(name=name).first()

    @classmethod
    def get_df_from_db(cls, session: Session) -> pd.DataFrame:
        # Query the data
        data = session.query(cls).all()
        # Create a DataFrame from the query result
        columns = list(cls.__table__.columns.keys())
        df = pd.DataFrame([{col: getattr(row, col) for col in columns} for row in data], columns=columns)
        return df

    @classmethod
    def insert_df(cls, session: Session, df: pd.DataFrame):
        try:
            # Convert DataFrame to list of dictionaries and insert all rows into the database table using the session
            data = df.to_dict(orient='records')
            session.bulk_insert_mappings(cls, data)
            session.commit()
            print(f"{cls.__name__} data inserted successfully!")
        except Exception as e:
            # Rollback the session in case of an error to discard the changes
            session.rollback()
            print(f"Error: {e}")


# Create a declarative base
Base = declarative_base(cls=BaseMixin)
