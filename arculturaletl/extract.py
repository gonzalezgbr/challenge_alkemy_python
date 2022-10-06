"""This module downloads the data files and stores them locally."""

from datetime import date, datetime
from pathlib import Path
import locale
import sys

from decouple import config
import requests


def make_filename(category):
    """Return filename as 'category-day_nbr-month_nbr-year_nbr.csv.'"""

    return f"{category}-{date.today().strftime('%d-%m-%Y')}.csv"

def make_filepath(category):
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

def make_url(url):
    """Adjust url from config to be able to download googlesheet as csv."""
    edit_idx = url.find('edit')
    return url[:edit_idx] + 'gviz/tq?tqx=out:csv'

def extract_data():
    """Downloads and locally saves csv files using urls from config file."""
    
    urls = {
        'museos' : config('URL_MUSEOS'),
        'bibliotecas' : config('URL_BIBLIOTECAS'),
        'cines' : config('URL_CINES')
    }
    for category, url in urls.items():
        downloadable_url = make_url(url)
        filepath = make_filepath(category) / make_filename(category)
        try:
            r = requests.get(downloadable_url, timeout=3)
        except requests.exceptions.RequestException as e:
            print(f'There was an error downloading the files: {e}', file=sys.stderr)
            exit()
        try:
            with open(filepath, 'wb') as f:
                f.write(r.content)
        except OSError:
            print('There was a problem saving the files locally.', file=sys.stderr)


if __name__ == '__main__':
    extract_data()
