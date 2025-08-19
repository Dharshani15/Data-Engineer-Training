from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from datetime import datetime

EXTRACT_FILE = "/tmp/simple_data.txt"
TRANSFORM_FILE = "/tmp/simple_data_transformed.txt"

def extract_data():
    print("Extracting data...")
    data = "5,10,15,20"
    with open(EXTRACT_FILE, "w") as f:
        f.write(data)

def transform_data():
    print("Transforming data...")
    with open(EXTRACT_FILE, "r") as f:
        numbers = f.read().split(",")
    numbers = [str(int(n) + 100) for n in numbers] 
    with open(TRANSFORM_FILE, "w") as f:
        f.write(",".join(numbers))


with DAG(
    dag_id="AIRFLOW-1",
    start_date=datetime(2025, 1, 1),
    schedule=None,  
    catchup=False,
    tags=["assignment", "etl"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

   
    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    
    load_task = BashOperator(
        task_id="load_data",
        bash_command=f"echo 'Final loaded data:' && cat {TRANSFORM_FILE}"
    )

    
    extract_task >> transform_task >> load_task
