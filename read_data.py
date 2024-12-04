import pandas as pd
import sqlite3
import glob
import os
def read():
    # Load CSV data into Pandas DataFrames
    AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
    customer_files = glob.glob(AIRFLOW_HOME+'/dags/Resources/customers/*.csv')

    transaction_files = glob.glob(AIRFLOW_HOME+'/dags/Resources/deposit_transactions/*.csv')
        # Combine customer data
    customers = pd.concat((pd.read_csv(f) for f in customer_files), ignore_index=True)
    # Combine transaction data
    transactions = pd.concat((pd.read_csv(f) for f in transaction_files), ignore_index=True)
    transactions['date'] = pd.to_datetime(transactions['date'])
    # Create a SQLite database and connect to it
    conn = sqlite3.connect('bank_data.db')
    # Load the data into SQL tables
    customers.to_sql('customers', conn, if_exists='replace', index=False)
    transactions.to_sql('transactions', conn, if_exists='replace', index=False)
read()