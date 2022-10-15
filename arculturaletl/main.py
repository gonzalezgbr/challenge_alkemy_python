# main.py

"""This module triggers the execution of the extract, transform and load processes."""

from pathlib import Path
from datetime import datetime
import logging
import sys

from decouple import config
import pandas as pd

from arculturaletl.extract import extract_data
from arculturaletl.load import make_engine, create_table, load_data
from arculturaletl.transform import load_datasets, make_master_dataset, make_summary_dataset, make_cine_dataset


def configure_logger():
    """Define log message format and logging level from config file."""

    log_msge = '%(asctime)s [%(levelname)s] %(module)s.py  %(funcName)s: %(message)s'
    log_level = config('LOG_LEVEL', 'INFO').upper()
    log_file = Path(__file__).parent.resolve() / 'logfile.log'
    logging.basicConfig(format=log_msge, level=log_level, handlers=[
                                            logging.FileHandler(log_file, encoding='utf8'),
                                            logging.StreamHandler(sys.stdout)])
    logging.info('Inicio de ejecución.')


if __name__ == '__main__':
    
    configure_logger()

    csv_filepaths = extract_data()
    #csv_filepaths = {
    #    'bibliotecas' : Path(__file__).parent.resolve() / 'arculturaletl/data/bibliotecas/2022-octubre/bibliotecas-07-10-2022.csv',
    #    'cines' : Path(__file__).parent.resolve() / 'arculturaletl/data/cines/2022-octubre/cines-07-10-2022.csv',
    #    'museos' : Path(__file__).parent.resolve() / 'arculturaletl/data/museos/2022-octubre/museos-07-10-2022.csv'
    #}
    
    if not len(csv_filepaths) == 3:
        logging.error('No se cuenta con los 3 archivos necesarios para generar las tablas.')
        sys.exit(1)
    
    # Load all dfs 
    dfs = load_datasets(csv_filepaths)
    
    # Create db engine
    engine = make_engine()

    # Make master df and load to bd table
    lugar_df = make_master_dataset(dfs)
    lugar_df['fecha_carga'] = datetime.now()
    create_table(engine, table_name='lugar')
    load_data(engine, table_name='lugar', df=lugar_df)

    # Make resumen df and load to bd table
    resumen_df = make_summary_dataset(dfs)
    resumen_df['fecha_carga'] = datetime.now()
    create_table(engine, table_name='resumen')
    load_data(engine, table_name='resumen', df=resumen_df)

    # Make cine df and load to bd table
    cine_df = make_cine_dataset(dfs['cines'])
    cine_df['fecha_carga'] = datetime.now()
    create_table(engine, table_name='cine')
    load_data(engine, table_name='cine', df=cine_df)

    engine.dispose()
    logging.info('Fin de ejecución.')