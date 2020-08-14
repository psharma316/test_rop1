import datetime
import os
import json

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
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
    'project_id': models.Variable.get('PROJECT_ID','dataproc-usecase-286310')
}

with models.DAG(
        'gcp_poc_automation5',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:


    create_dataproc_cluster = BashOperator(
        task_id='create_dataproc_cluster',
        bash_command='gcloud beta dataproc clusters create dataproc --region us-west4 --subnet default --zone us-west4-a --master-machine-type n1-standard-1 --master-boot-disk-size 100 --num-workers 2 --worker-machine-type n1-standard-1 --worker-boot-disk-size 100 --image-version 1.3-debian10 --project dataproc-usecase-286310',
        dag=dag)



    dataproc_hive_create_db = DataProcHiveOperator(
        task_id='create_db',
        gcp_conn_id='google_cloud_default', 
        query="CREATE DATABASE IF NOT EXISTS default_autotestbqdb9 LOCATION 'gs://dataproc-staging-us-central1-788915459809-fk8wm6rc/HQL/';",
        cluster_name='dataproc',
        region='us-west4',
        dag=dag)


    dataproc_hive_create_table_par = DataProcHiveOperator(
        task_id='dataproc_hive_create_table_par',
        gcp_conn_id='google_cloud_default', 
        query="CREATE EXTERNAL TABLE IF NOT EXISTS default.chicago_taxi_trips_parquet_autotestbq9(unique_key   STRING,taxi_id  STRING,trip_start_timestamp  STRING, trip_end_timestamp  STRING, trip_seconds  STRING, trip_miles   STRING, pickup_census_tract  STRING, dropoff_census_tract  STRING, pickup_community_area  STRING, dropoff_community_area  STRING, fare  STRING, tips  STRING, tolls  STRING, extras  STRING, trip_total  STRING, payment_type  STRING, company  STRING, pickup_latitude  STRING, pickup_longitude  STRING, pickup_location  STRING,dropoff_latitude  STRING, dropoff_longitude  STRING, dropoff_location  STRING) STORED AS PARQUET location 'gs://dataproc-staging-us-central1-788915459809-fk8wm6rc/HQL/PARQUET/';",
        cluster_name='dataproc',
        region='us-west4',
        dag=dag)

		
    dataproc_hive_create_table_csv = DataProcHiveOperator(
        task_id='dataproc_hive_create_table_csv',
        gcp_conn_id='google_cloud_default', 
        query="CREATE EXTERNAL TABLE IF NOT EXISTS default.chicago_taxi_trips_csv_autotestbq9( unique_key   STRING,taxi_id  STRING,trip_start_timestamp  STRING,trip_end_timestamp  STRING,trip_seconds  STRING,trip_miles   STRING,pickup_census_tract  STRING,dropoff_census_tract  STRING,pickup_community_area  STRING,dropoff_community_area  STRING,fare  STRING,tips  STRING,tolls  STRING,extras  STRING,trip_total  STRING,payment_type  STRING,company  STRING,pickup_latitude  STRING,pickup_longitude  STRING,pickup_location  STRING,dropoff_latitude  STRING,dropoff_longitude  STRING,dropoff_location  STRING)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location 'gs://dataproc-staging-us-central1-788915459809-fk8wm6rc/HQL/CSV/'; ",
        cluster_name='dataproc',
        region='us-west4',
        dag=dag)

    dataproc_load_csv_table = BashOperator(
        task_id='dataproc_load_csv_table',
        bash_command='gsutil -m cp -R gs://dp-landing-area/chicago_taxi_trips-00000000000[0-4]*.csv gs://dataproc-staging-us-central1-788915459809-fk8wm6rc/HQL/CSV/',
        dag=dag)


    dataproc_load_par_table = DataProcHiveOperator(
        task_id='dataproc_load_par_table',
        gcp_conn_id='google_cloud_default', 
        query="INSERT INTO TABLE default.chicago_taxi_trips_parquet_autotestbq9 SELECT * FROM default.chicago_taxi_trips_csv_autotestbq9;",
        cluster_name='dataproc',
        region='us-west4',
        dag=dag)
        
   
    dataproc_hive_count_table_csv = DataProcHiveOperator(
        task_id='dataproc_hive_count_table_csv',
        gcp_conn_id='google_cloud_default', 
        query="select count(*) from default.chicago_taxi_trips_csv_autotestbq9",
        cluster_name='dataproc',
        region='us-west4',
        dag=dag)

    delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        cluster_name='dataproc',
        region='us-west4',
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)


		
    load_parquet_bqt = GoogleCloudStorageToBigQueryOperator(
        task_id='load_parquet_bqt',
        bucket='dphivedb',
        source_objects=['HQL/PARQUET/*'],
        schema_fields=None,
        schema_object=None,
        source_format='parquet',
        destination_project_dataset_table='bqdataset.test3',
        bigquery_conn_id='bigquery_default',
        google_cloud_storage_conn_id='google_cloud_default',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        delegate_to=None,
        udf_config=None,
        use_legacy_sql=True,
        maximum_billing_tier=None,
        maximum_bytes_billed=None,
        create_disposition='CREATE_IF_NEEDED',
        schema_update_options=(),
        query_params=None,
        labels=None,
        priority='INTERACTIVE',
        time_partitioning=None,
        api_resource_configs=None,
        cluster_fields=None,
        location=None,
        encryption_configuration=None,
        dag=dag)


    rm_par_file = BashOperator(
        task_id='rm_par_file',
        bash_command='gsutil -m rm -R gs://dataproc-staging-us-central1-788915459809-fk8wm6rc/HQL/PARQUET/*;',
        dag=dag)


    count_bq_table = BashOperator(
        task_id='count_bq_table',
        bash_command= "bq query --use_legacy_sql=false \'SELECT COUNT(*) FROM datasetname.test3'",
        dag=dag)


    create_dataproc_cluster >> dataproc_hive_create_db >> dataproc_hive_create_table_csv >> dataproc_hive_create_table_par >> dataproc_load_csv_table >> dataproc_load_par_table >> dataproc_hive_count_table_csv >> delete_dataproc_cluster >> load_parquet_bqt >> rm_par_file >> count_bq_table 