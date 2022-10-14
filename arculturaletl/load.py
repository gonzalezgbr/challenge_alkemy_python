# load.py

"""This module holds the DB utilities to connect and load data from df to DB."""

from pathlib import Path

from decouple import config
from sqlalchemy import create_engine, Engine, exc, text
import pandas as pd


def make_connection_uri() -> str:
    """Returns the connection string built from .env vars"""

    return f"{config('DB_DRIVER')}://{config('USER')}:{config('PASSWORD')}@{config('HOST')}:{config('PORT')}/{config('DB')}"
 

def make_engine() -> Engine:
    """Return engine to connect to DB."""

    return create_engine(make_connection_uri())


def create_table(engine: Engine, table_name: str):
    """Create table from sql script in sql folder."""
    
    file_path = Path(__file__).parent.resolve() / 'sql' / table_name / '.sql'
    
    try:
        with open(file_path) as sql_file:
            sql_str = sql_file.read()
        with engine.connect() as conn:
            try:
                conn.execute(text(sql_str)) 
            except exc.ProgrammingError as e:
                print(f'{table_name} ya existe')
    except:
        print('File not found')    


def load_data(engine: Engine, table_name: str, df: pd.DataFrame):
    """Load a pandas DF into a postgreSQL table."""
    
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    
