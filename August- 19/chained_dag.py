from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from datetime import datetime

# Define message path
MESSAGE_PATH = "/tmp/message.txt"

# Example Python functions
def generate_message():
    with open(MESSAGE_PATH, "w") as f:
        f.write("Hello from Airflow!")

def read_message():
    with open(MESSAGE_PATH, "r") as f:
        content = f.read()
        print(content)

# Define DAG
with DAG(
    dag_id="chained_dag",
    start_date=datetime(2025, 8, 19),
    schedule=None,
    catchup=False,
    tags=["example"]
) as dag:

    generate = PythonOperator(
        task_id="generate_message",
        python_callable=generate_message
    )

    simulate_save = BashOperator(
        task_id="simulate_file_confirmation",
        bash_command=f"echo 'Message saved at {MESSAGE_PATH}'"
    )

    read = PythonOperator(
        task_id="read_message",
        python_callable=read_message
    )

    # Set task dependencies
    generate >> simulate_save >> read
