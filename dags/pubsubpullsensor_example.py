from airflow import DAG
import datetime
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor

DEFAULT_DAG_ARGS = {
    'owner': 'Saipul',
    'depends_on_past': False,
    'start_date': datetime.datetime(2020, 9, 10, 0, 0),
    'email': ['msaipulrx@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'pubsub_pullsensor',
    schedule_interval='@daily',
    default_args=DEFAULT_DAG_ARGS)

subscription = 'latihan-subs'
gcp_conn_id = 'google_cloud_default'
project_id = 'sekolah-bigdata-3'


#define task

task_start = BashOperator(
    task_id="start_task",
    bash_command='echo start',
    dag=dag
)

pull_message = PubSubPullSensor(
    task_id="pull_message_pubsub",
    ack_messages=True,
    project_id=project_id,
    gcp_conn_id=gcp_conn_id,
    subscription=subscription,
    dag=dag
)

task_end = BashOperator(
    task_id="end_task",
    bash_command='echo end',
    dag=dag
)

task_start >> pull_message >> task_end