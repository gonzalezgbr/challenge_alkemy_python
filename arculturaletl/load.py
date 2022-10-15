# load.py

"""This module holds the DB utilities to connect and load data from df to DB."""

from pathlib import Path
from typing import Optional
import logging
import sys

from decouple import config, UndefinedValueError
from sqlalchemy import create_engine, engine, exc, text 
import pandas as pd


def make_connection_uri() -> Optional[str]:
    """Returns the connection string built from .env vars"""
    
    try:
        conn_uri = f"{config('DB_DRIVER')}://{config('USER')}:{config('PASSWORD')}@{config('HOST')}:{config('PORT')}/{config('DB')}"
        return conn_uri
    except UndefinedValueError:
        logging.error('No se encuentran los datos de conexión a la BD en archivo .env.')
        sys.exit(1)

def make_engine() -> engine.Engine:
    """Return engine to connect to DB."""

    return create_engine(make_connection_uri())


def create_table(engine: engine.Engine, table_name: str):
    """Create table from sql script in sql folder."""
    
    filename = table_name + '.sql'
    file_path = Path(__file__).parent.resolve() / 'sql' / filename

    try:
        with open(file_path) as sql_file:
            sql_str = sql_file.read()
        with engine.connect() as conn:
            try:
                conn.execute(text(sql_str)) 
                logging.info(f'Tabla {table_name} creada en la BD correctamente.')
            except exc.ProgrammingError as e:
                logging.warning(f'Tabla {table_name} ya existe en la BD.')
    except:
        logging.error(f'Error en los archivos.sql de creación de tablas o en los datos de conexión.')
        sys.exit(1)


def load_data(engine: engine.Engine, table_name: str, df: pd.DataFrame):
    """Load a pandas DF into a postgreSQL table."""
    
    result = df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    if result:
        logging.info(f'Tabla {table_name} cargada correctamente.')
    
