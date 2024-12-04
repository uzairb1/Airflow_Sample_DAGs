import sqlalchemy
import pandas as pd
import sqlite3
import os
import logging
def results():
    # Create a SQLAlchemy engine
    engine = sqlalchemy.create_engine('sqlite:///bank_data.db')

    # Calculate the highest account balance by country as of today
    AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
    fd = open(AIRFLOW_HOME+'/dags/sql/account_balance_by_country.sql','r')
    highest_balance_query = fd.read()
    fd.close()
    conn = sqlite3.connect('bank_data.db')

    highest_balance_result = pd.read_sql(highest_balance_query, conn)
    logging.info("Highest Balance Country: ", highest_balance_result)

    # Calculate end-of-month balances per country from 2019 onwards
    fd = open(AIRFLOW_HOME+'/dags/sql/end_of_month_account_balance.sql','r')
    monthly_balance_query = fd.read()
    fd.close()

    monthly_balance_result = pd.read_sql(monthly_balance_query, conn)
    for country in monthly_balance_result['country'].unique():
        country_df = monthly_balance_result[monthly_balance_result['country'] == country]
        country_df.to_csv(AIRFLOW_HOME+f'/dags/results/{country}_monthly_balance.csv', index=False)
