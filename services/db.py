from sqlalchemy import create_engine
from config.config import (
    DB_HOST,
    DB_PORT,
    DB_USER,
    DB_PASSWORD,
    DB_NAME
)
import psycopg2
import pandas as pd


# Database connection parameters
db_params = {
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "host": DB_HOST,
    "port": DB_PORT
}

engine = create_engine(
        f'postgresql+psycopg2://'
        f'{db_params["user"]}:{db_params["password"]}@'
        f'{db_params["host"]}:{db_params["port"]}/'
        f'{db_params["dbname"]}')


def get_db_connection() -> psycopg2.extensions.connection:
    # Establish database connection
    conn = psycopg2.connect(**db_params)
    # Create a cursor object
    cursor = conn.cursor()
    return conn


def insert_data(df: pd.DataFrame, table_name: str) -> None:
    # Insert DataFrame records into PostgreSQL table
    df.to_sql(table_name, engine, if_exists='append', index=False, method='multi')
    print(f"Data has been inserted into the '{table_name}' table.")
