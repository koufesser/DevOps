from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'my_dag',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 27),
    catchup=False
)

spark_job = SparkSubmitOperator(task_id='custom_spark',
                                application=f'/opt/airflow/spark/custom_spark.py',
                                name='custom_spark',
                                conn_id='spark_local',
                                files='/opt/airflow/inputs/Bible.txt',
                                verbose=True,
                                dag=dag)

spark_job
