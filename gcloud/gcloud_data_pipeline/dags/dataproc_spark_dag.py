from datetime import timedelta
from airflow.utils.dates import days_ago
from ..plugins.dataproc_airflow_builder import DataprocSparkJob

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DataprocSparkJob class with your specific parameters
spark_job = DataprocSparkJob(
    dag_id='example_spark_job',
    gcp_project_id='useful-tempest-398111',
    pyspark_uri='gs://your-bucket/path-to-your-main-pyspark-script.py',
    py_files_uris=['gs://your-bucket/path-to-dependencies.zip'],
    cluster_name='your-cluster-name',
    cluster_region='your-region',
    num_workers=2,
    machine_type='n1-standard-2',
    default_args=default_args,
)

# Retrieve the DAG from the DataprocSparkJob instance
dag = spark_job.get_dag()


