"""This module downloads the data files and stores them locally."""

from datetime import date, datetime
from pathlib import Path
import locale
import logging
import sys
from typing import Dict, Optional

from decouple import config, UndefinedValueError
import requests


def make_filename(category: str) -> str:
    """Return filename as 'category-day_nbr-month_nbr-year_nbr.csv.'"""

    return f"{category}-{date.today().strftime('%d-%m-%Y')}.csv"

def make_filepath(category: str) -> str:
    """Return full filepath as 'data/category/year_nbr-month_name', create folders if 
    they don't exist.
    """

    # set locale to get month name from date, in spanish
    locale.setlocale(locale.LC_ALL, 'es_AR')
    month = datetime.strptime(date.today().strftime('%m'), '%m')
    month_name = month.strftime('%B')
    
    basepath = Path(__file__).parent.resolve() / 'data'
    filepath = basepath / category / f"{date.today().year}-{month_name}"
    if not filepath.exists():
        filepath.mkdir(parents=True, exist_ok=False)
    
    return filepath


def make_url(url: str) -> Optional[str]:
    """Adjust url from config to be able to download googlesheet as csv."""
    edit_idx = url.find('edit')
    if edit_idx != -1:
        return url[:edit_idx] + 'gviz/tq?tqx=out:csv'
    else:
        logging.error('Alguna url de descarga no es correcta')
        sys.exit(1)


def extract_data() -> Dict[str, Path]:
    """Download and locally save csv files using urls from config file. 
    Return csv filepaths."""
    
    csv_filepaths = {}
    try:    
        urls = {
            'museos' : config('URL_MUSEOS'),
            'bibliotecas' : config('URL_BIBLIOTECAS'),
            'cines' : config('URL_CINES')
        }
    except UndefinedValueError as error:
        logging.error('No se encontraron las url de descarga de archivos en archivo .env.')
        sys.exit(1)

    for category, url in urls.items():
        downloadable_url = make_url(url)
        filepath = make_filepath(category) / make_filename(category)
        try:
            response = requests.get(downloadable_url, timeout=3)
        except requests.exceptions.RequestException as error:
            logging.error(f'Error descargando los archivos: {error}')
            sys.exit(1)
        try:
            with open(filepath, 'wb') as csv_file:
                csv_file.write(response.content)
                csv_filepaths[category] = filepath
                logging.info(f'Archivo {category} descargado y almacenado localmente.')
        except OSError as error:
            logging.error('Error al guardar los archivos localmente: {error}')
            sys.exit(1)
    
    return csv_filepaths
