"""Example Airflow DAG that creates a Cloud Dataproc cluster, runs the Hadoop
wordcount example, and deletes the cluster.

This DAG relies on three Airflow variables
https://airflow.apache.org/concepts.html#variables
* gcp_project - Google Cloud Project to use for the Cloud Dataproc cluster.
* gce_zone - Google Compute Engine zone where Cloud Dataproc cluster should be
  created.
* gcs_bucket - Google Cloud Storage bucket to use for result of Hadoop job.
  See https://cloud.google.com/storage/docs/creating-buckets for creating a
  bucket.
"""

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
    'project_id': models.Variable.get('PROJECT_ID','dataproc-usecase-276215')
}

with models.DAG(
        'automation_initialization15',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    # Create a Cloud Dataproc cluster.
    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        cluster_name='dataproc',
	#PROJECT_ID=models.Variable.get('PROJECT_ID','dataproc-usecase-276215'),
	PROJECT_ID='dataproc-usecase-276215',
	region='us-west1',
	num_masters=1,
        num_workers=2,
	zone='us-west1-b',
        master_machine_type='n1-standard-1',
	master_disk_size = 100,
	worker_disk_size = 100,
	num_preemptible_workers=0,
        worker_machine_type='n1-standard-1',
	idle_delete_ttl =1800,
	subnetwork_uri='ctl',
	optional_components=['PRESTO','SOLR','RANGER'],
	service_account_scopes=['https://www.googleapis.com/auth/cloud-platform'],
        #service_account_scopes=['default'],
	internal_ip_only='false',
	#init_actions_uris=['gs://goog-dataproc-initialization-actions-us-west1/cloud-sql-proxy/cloud-sql-proxy.sh'],
        #metadata ={'kms-key-uri': 'projects/dataproc-usecase-276215/locations/global/keyRings/my-key-ring/cryptoKeys/migration-key', 'db-hive-password-uri': 'gs://secrectkeybucket2/hive-password.encrypted'})
        metadata= {'kms-key-uri': 'projects/dataproc-usecase-276215/locations/global/keyRings/my-key-ring/cryptoKeys/migration-key', 'db-hive-password-uri': 'gs://secrectkeybucket2/hive-password.encrypted','use-cloud-sql-private-ip': 'true','db-admin-password-uri': 'gs://secrectkeybucket2/admin-password.encrypted', 'hive-metastore-instance': 'dataproc-usecase-276215:us-west1:hive-metadata'},        
        init_actions_uris=['gs://dataproc-staging-us-west4-17809115036-8cg5zgf1/cloud-sql-proxy/cloud-sql-proxy.sh'],        
        init_action_timeout="10m")
        
		
		
    # Run the Hive job on the Cloud Dataproc cluster
    run_dataproc_hive_create_db = DataProcHiveOperator(
        task_id='create_db',
        gcp_conn_id='google_cloud_default', 
        query_uri="gs://data-bucket-hive2/HQL/create_db.hql",
        cluster_name='dataproc',
        region='us-west1',
        dag=dag)
	
    # Define DAG dependencies.
    #create_dataproc_cluster >> run_dataproc_hive >> delete_dataproc_cluster
    create_dataproc_cluster >> run_dataproc_hive_create_db 