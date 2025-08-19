from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from datetime import datetime, timedelta
import json, logging, random


# Default arguments
default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "email": ["alerts@datacorp.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Functions for PythonOperator
def pull_data(**kwargs):
    # Simulate pulling data from API / DB
    data = {"id": 1, "value": random.randint(10, 100), "timestamp": str(datetime.now())}
    print(f"Pulled Data: {data}")
    return data

def validate_data(**kwargs):
    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids="pull_data_task")
    
    # Business rule: value must not exceed 80
    if data["value"] > 80:
        raise ValueError(f"Validation failed! Value {data['value']} exceeds threshold 80")
    
    result = {"status": "success", "checked_value": data["value"]}
    with open("/tmp/audit_result.json", "w") as f:
        json.dump(result, f)
    print("Validation successful. Result stored in /tmp/audit_result.json")
    return result

def log_results(**kwargs):
    ti = kwargs["ti"]
    result = ti.xcom_pull(task_ids="validate_data_task")
    print(f"Logging Results: {result}")

# DAG Definition
with DAG(
    dag_id="data_audit_dag",
    default_args=default_args,
    description="Event-Driven Data Audit DAG",
    schedule="@hourly",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["audit", "data"],
) as dag:

    # Task 1: Data Pull
    pull_data_task = PythonOperator(
        task_id="pull_data_task",
        python_callable=pull_data
    )

    # Task 2: Validation
    validate_data_task = PythonOperator(
        task_id="validate_data_task",
        python_callable=validate_data
    )

    # Task 3: Log Results
    log_results_task = PythonOperator(
        task_id="log_results_task",
        python_callable=log_results
    )

    # Task 4: Final Status Update (BashOperator)
    final_status_task = BashOperator(
        task_id="final_status_task",
        bash_command="echo 'Audit Completed at {{ ds }}'"
    )

    # Task dependencies
    pull_data_task >> validate_data_task >> log_results_task >> final_status_task
