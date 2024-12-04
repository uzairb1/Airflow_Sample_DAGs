import pandas as pd
import requests
import sqlite3
from datetime import datetime
import sqlalchemy
import glob
import xml.etree.ElementTree as ET

def fetch_ecb_exchange_rates():
    url = 'https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.xml'
    response = requests.get(url)
    tree = ET.ElementTree(ET.fromstring(response.content))
    root = tree.getroot()

    # Namespace dictionary
    ns = {'ns': 'http://www.ecb.int/vocabulary/2002-08-01/eurofxref'}

    # List to store exchange rates
    rates = []

    for cube in root.findall('.//ns:Cube[@time]', ns):
        date = cube.get('time')
        for rate in cube.findall('.//ns:Cube[@currency="GBP"]', ns):
            gbp_to_eur = float(rate.get('rate'))
            rates.append({'date': date, 'gbp_to_eur': gbp_to_eur})

    return pd.DataFrame(rates)

def exchange_rates():
    # Fetch the exchange rates
    exchange_rates_df = fetch_ecb_exchange_rates()
    exchange_rates_df['date'] = pd.to_datetime(exchange_rates_df['date'])

    # Create a SQLite database and connect to it
    conn = sqlite3.connect('bank_data.db')

    exchange_rates_df.to_sql('exchange_rates', conn, if_exists='replace', index=False)

exchange_rates()