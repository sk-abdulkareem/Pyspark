from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime

with DAG(
    "orders_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
) as dag:

    job = DatabricksSubmitRunOperator(
        task_id="run_orders_etl",
        json={
            "python_file_task": {
                "python_file": "dbfs:/jobs/orders_etl.py",
                "parameters": ["prod"]
            },
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 4
            }
        }
    )
