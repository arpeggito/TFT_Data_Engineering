from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
from tft_project import tft_pipeline

args = {
    'owner' : 'Andres Medina',
    'start_date': days_ago(1)
}
dag = DAG(
    dag_id = 'test_workflow_tft',
    default_args=args,
    schedule_interval='@daily'

)

with dag:

    tft_project = PythonOperator(

        task_id='tft_pipeline',

        python_callable=tft_pipeline,
    )

# Set the dependencies between the tasks

tft_pipeline
