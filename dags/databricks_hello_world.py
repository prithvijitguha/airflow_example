# databricks_hello_world.py
"""This is an example DAG to show how we can orchestrate a notebook task with
a job cluster with Databricks

Make sure to replace notebook_path with the actual path to be used
"""
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.dates import days_ago


with DAG('databricks_airflow_dag',
    start_date = days_ago(2),
    schedule_interval = None,
    ) as dag:
    new_cluster = {"spark_version": "13.2.x-scala2.12", "num_workers": 2}
    notebook_task = {
        "notebook_path": "/Users/airflow@example.com/databricks_airflow",
    }
    notebook_run = DatabricksSubmitRunOperator(
        task_id="notebook_run", new_cluster=new_cluster, notebook_task=notebook_task
    )