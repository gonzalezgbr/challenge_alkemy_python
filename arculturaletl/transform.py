"""This module loads the local files, transforms the datasets according to the
 requirements and calls the load module."""

from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd


def get_file_category(filename):
    """Return the first word of the filename, which indicates its category."""

    first_dash = filename.find('-')
    return filename[:first_dash]


def load_datasets(csv_filepaths):
    """Load all dfs in a dict to pass into each of the transform functions."""
    
    dfs = {}
    for category, filepath in csv_filepaths.items():
        #category = get_file_category(file.stem)
        df = pd.read_csv(filepath, encoding='utf8')
        dfs[category] = df
    return dfs


def make_phone_number(row):
    """To be applied on df row. Make phone number from cod_area and telefono, 
    cleaning each value first."""
    
    if not pd.isnull(row['Teléfono']):
        # cast to str
        tel = str(row['Teléfono']).strip()
        # delete final .0 if it's there
        if tel.find('.0') != -1:
            tel = tel[:tel.find('.0')]
        
        # check cod_area, if not null, concatenate with space, otherwise use just Teléfono
        if not pd.isnull(row['Cod_tel']):
            cod = str(row['Cod_tel']).strip()
            if cod.find('.0') != -1:
                cod = cod[:cod.find('.0')]
            row['telefono'] = cod + ' ' + tel
        else:
            row['telefono'] = tel
    
    else:
    # if telefonois null, then cod_area is of no use
        row['telefono'] = np.nan
    
    return row


def unify_province_names(row):
    """To be applied on df row. Some provinces have two different names, unify them."""
    
    if row['provincia'] == 'Neuquén\xa0':
        row['provincia'] = 'Neuquén'
    elif row['provincia'] == 'Santa Fe':
        row['provincia'] = 'Santa Fé'
    elif row['provincia'] == 'Tierra del Fuego':
        row['provincia'] = 'Tierra del Fuego, Antártida e Islas del Atlántico Sur'
    
    return row


def convert_text_nulls_to_nan(row):
    """To be applied on df row. Some rows have textual nulls, convert them to NaN."""
    
    nulls = ['sin dirección', 's/d']
    if str(row['domicilio']).strip().lower() in nulls:
        row['domicilio'] = np.nan
    if str(row['web']).strip().lower() in nulls:
        row['web'] = np.nan
    if str(row['mail']).strip().lower() in nulls:
        row['mail'] = np.nan
    
    return row


def make_master_dataset(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Return datatset with data from all 3 files and required fields."""
    
    # 1) Rename columns so all dfs match
    columns = ['cod_localidad', 'id_provincia', 'id_departamento', 'categoria', 'provincia', 
                'localidad','nombre','domicilio', 'codigo_postal', 'Cod_tel', 'Teléfono', 'mail', 
                'web' ]
    bibliotecas_columns = ['Cod_Loc', 'IdProvincia', 'IdDepartamento', 'Categoría', 
                        'Provincia', 'Localidad','Nombre','Domicilio', 'CP', 'Cod_tel', 
                        'Teléfono', 'Mail', 'Web' ]
    cines_columns = ['Cod_Loc', 'IdProvincia', 'IdDepartamento', 'Categoría', 'Provincia', 
                        'Localidad','Nombre','Dirección', 'CP', 'cod_area', 'Teléfono', 
                        'Mail', 'Web' ]
    museos_columns = ['Cod_Loc', 'IdProvincia', 'IdDepartamento', 'categoria', 'provincia', 
                        'localidad','nombre','direccion', 'CP', 'cod_area', 'telefono', 
                        'Mail', 'Web' ]
    dfs['bibliotecas'].rename(columns=dict(zip(bibliotecas_columns, columns)), 
                            inplace=True)
    dfs['cines'].rename(columns=dict(zip(cines_columns, columns)), inplace=True)
    dfs['museos'].rename(columns=dict(zip(museos_columns, columns)), inplace=True)
    # concatenate dfs into one
    master_df = pd.concat([dfs['bibliotecas'][columns], 
                            dfs['cines'][columns], 
                            dfs['museos'][columns]])
    
    # 2) Make phone number: combine cod and tel in a single column of str type
    # Delete cod_tel and telefono columns
    master_df = master_df.apply(make_phone_number, axis='columns')
    master_df.drop(['Cod_tel', 'Teléfono'], inplace=True, axis='columns')

    # 3) Unify province names in some rows so they match
    master_df = master_df.apply(unify_province_names, axis='columns')
    
    # 4) Correct province code for one row: Salta has code 58, should be 66
    master_df.loc[(master_df['provincia'] == 'Salta') & (master_df['id_provincia'] == 58), 
                    'id_provincia'] = 66

    # 5) Unify null values for Domicilio, Web and Mail ('Sin direccion', 's/d)
    master_df = master_df.apply(convert_text_nulls_to_nan, axis='columns')

    # 6) Some CP values have .0 at the end, remove them
    master_df['codigo_postal'] = master_df['codigo_postal'].map(lambda x: str(x)[:-2] 
                                                    if str(x).find('.0') != -1 else x)

    return master_df


if __name__ == '__main__':
    print(Path.cwd())
    csv_filepaths = {
        'bibliotecas' : Path(__file__).parent.resolve() / 'data/bibliotecas/2022-octubre/bibliotecas-07-10-2022.csv',
        'cines' : Path(__file__).parent.resolve() / 'data/cines/2022-octubre/cines-07-10-2022.csv',
        'museos' : Path(__file__).parent.resolve() / 'data/museos/2022-octubre/museos-07-10-2022.csv'
    }
    dfs = load_datasets(csv_filepaths)
    master_df = make_master_dataset(dfs)
    master_df.to_csv('victory.csv', encoding='utf8')
    #load_into_table('lugar', master_df)
    #make_totals_dataset()
    #make_cinemas_dataset()