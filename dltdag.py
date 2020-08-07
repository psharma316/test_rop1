import datetime
import os

from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from airflow.utils import trigger_rule
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': models.Variable.get('PROJECT_ID','dataproc-usecase-276411')
}

with models.DAG(
        'EndToEndDAG14',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
		
    run_dataproc_hive_create_db = DataProcHiveOperator(
        task_id='create_db',
        gcp_conn_id='google_cloud_default', 
        query="CREATE DATABASE IF NOT EXISTS default3 LOCATION 'gs://dphivedb/HQL/CSV/test/';",
        cluster_name='dataproc',
        region='us-west1',
        dag=dag)

    delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        cluster_name='dataproc',
        region='us-west1',
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    run_dataproc_hive_create_db >> delete_dataproc_cluster 