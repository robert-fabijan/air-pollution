

# Example usage:
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

spark_job = DataprocSparkJob(
    dag_id='example_spark_job',
    gcp_project_id='your-gcp-project-id',
    pyspark_uri='gs://your-bucket/path-to-your-main-pyspark-script.py',
    py_files_uris=['gs://your-bucket/path-to-dependencies.zip'],
    cluster_name='your-cluster-name',
    cluster_region='your-region',
    num_workers=2,
    machine_type='n1-standard-2',
    default_args=default_args,
)

dag = spark_job.get_dag()