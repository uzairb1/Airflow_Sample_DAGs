from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import sqlite3
from calculate_results import results
from read_data import read 
from fetch_exchange_rate import exchange_rates
from airflow.operators.dummy import DummyOperator
from airflow.hooks.S3_hook import S3Hook

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'monthly_balance_to_s3',
    default_args=default_args,
    description='Calculate monthly balances and upload to S3',
    schedule_interval='@monthly',
)

start = DummyOperator(task_id='start', dag=dag)

def upload_to_s3():
    #replace my_s3_conn with your s3 access key
    s3 = S3Hook('my_s3_conn')
    conn = sqlite3.connect('../part_1/bank_data.db')
    res = conn.execute('select distinct country from customers')
    countries = res.fetchall()
    for country in countries:
        s3.load_file(f'/results/{country[0]}_monthly_balance.csv', f'{country}_monthly_balance.csv', bucket_name='my_bucket', replace=True)

fetch_data = PythonOperator(
    task_id = 'read_data_from_resources',
    python_callable=read,
    dag=dag,
)

# Define the tasks
fetch_exchange_rate_task = PythonOperator(
    task_id='fetch_exchange_rate_to_db',
    python_callable=exchange_rates,
    dag=dag,
)

calculate_and_export_balances_task = PythonOperator(
    task_id='calculate_and_export_balances',
    python_callable=results,
    dag=dag,
)

upload_to_s3_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    dag=dag,
)

# Set task dependencies
start >> fetch_data >> fetch_exchange_rate_task >> calculate_and_export_balances_task >> upload_to_s3_task
