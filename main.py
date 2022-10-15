# main.py

"""This module triggers the execution of the extract, transform and load processes."""

from pathlib import Path
from datetime import datetime

import pandas as pd

from arculturaletl.extract import extract_data
from arculturaletl.load import make_engine, create_table, load_data
from arculturaletl.transform import load_datasets, make_master_dataset, make_summary_dataset, make_cine_dataset


if __name__ == '__main__':
    # csv_filepaths = extract_data()
    csv_filepaths = {
        'bibliotecas' : Path(__file__).parent.resolve() / 'arculturaletl/data/bibliotecas/2022-octubre/bibliotecas-07-10-2022.csv',
        'cines' : Path(__file__).parent.resolve() / 'arculturaletl/data/cines/2022-octubre/cines-07-10-2022.csv',
        'museos' : Path(__file__).parent.resolve() / 'arculturaletl/data/museos/2022-octubre/museos-07-10-2022.csv'
    }
    
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