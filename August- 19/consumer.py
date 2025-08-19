from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime
import os

def read_file():
    path = '/tmp/data.txt'
    if os.path.exists(path):
        with open(path, 'r') as f:
            print("Read from file:", f.read())
    else:
        raise FileNotFoundError("File not found. Please run Producer DAG first.")

with DAG(
    dag_id="consumer_dag",  # fixed quotes
    start_date=datetime(year=2023, month=1, day=1),
    schedule=None,          # Airflow 2.9+ syntax
    catchup=False,
    tags=['example']
) as dag:

    read_task = PythonOperator(
        task_id='read_file_task',
        python_callable=read_file
    )
