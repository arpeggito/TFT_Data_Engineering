from airflow import DAG
from airflow.operators.PythonOperator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
import Python_TFT_project

args = {
    'owner' : 'Andres Medina',
    'start_date': days_ago(1)
}
dag = DAG(
    dag_id = 'First_dag',
    default_args=args,
    schedule_interval='@daily'

)

with dag:

    TFT_Script = PythonOperator(

        task_id='Python_TFT_project',

        python_callable=Python_TFT_project,

    )

# Set the dependencies between the tasks

TFT_Script