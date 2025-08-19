from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from datetime import datetime, timedelta
import json, logging, random

AUDIT_FILE = "/tmp/audit_result.json"

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email": ["alert@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def pull_data():
    logging.info("Pulling data from external source...")
    data = {"timestamp": str(datetime.now()), "value": random.randint(50, 150)}
    logging.info(f"Data pulled: {data}")
    return data

def validate_data(ti):
    data = ti.xcom_pull(task_ids="pull_data_task")
    logging.info("Validating data...")
    
    if data["value"] > 100:
        result = {"status": "FAIL", "value": data["value"], "timestamp": data["timestamp"]}
        logging.error(f"Audit failed: {result}")
       
        with open(AUDIT_FILE, "w") as f:
            json.dump(result, f)
     
        raise ValueError("Audit validation failed!")
    else:
        result = {"status": "PASS", "value": data["value"], "timestamp": data["timestamp"]}
        logging.info(f"Audit passed: {result}")
        
        with open(AUDIT_FILE, "w") as f:
            json.dump(result, f)

log_audit = BashOperator(
    task_id="log_audit",
    bash_command=f"echo 'Audit result stored at {AUDIT_FILE}' && cat {AUDIT_FILE}"
)

def final_status():
    with open(AUDIT_FILE, "r") as f:
        result = json.load(f)
    logging.info(f"Final Audit Status: {result['status']}")

with DAG(
    dag_id="data_audit_dag",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["audit", "event-driven"]
) as dag:

    pull_data_task = PythonOperator(
        task_id="pull_data_task",
        python_callable=pull_data
    )

    validate_task = PythonOperator(
        task_id="validate_data_task",
        python_callable=validate_data,
        provide_context=True
    )

    final_status_task = PythonOperator(
        task_id="final_status_task",
        python_callable=final_status
    )

    pull_data_task >> validate_task >> log_audit >> final_status_task
