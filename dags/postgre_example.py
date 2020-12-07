from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

default_args = {
    "owner": "saipul",
    "start_date": datetime(2020, 11, 28),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "msaipulrx@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def retrieve_data():
    
    sql='SELECT * FROM customer'
    select_table = PostgresHook(postgres_conn_id='postgres2')
    connection = select_table.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    sources = cursor.fetchall()
    for source in sources:
        print("Source : {0} - activated: {1}".format(source[0], source[1]))
    return sources


with DAG(dag_id="postgre_exercises", schedule_interval="@once", default_args=default_args, catchup=False) as dag:

    create_table = PostgresOperator(
        task_id='createTable',
        postgres_conn_id='postgres2',
        sql='sql/create_table_customer.sql'
    )

    insert_table = PostgresOperator(
        task_id='insertTable',
        postgres_conn_id='postgres2',
        sql='sql/insert_table_customer.sql'
    )

    retrieve_data = PythonOperator(
        task_id='selectTable',
        python_callable=retrieve_data
    )

    create_table >> insert_table >> retrieve_data

