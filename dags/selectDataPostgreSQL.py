from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

default_args = {
    "owner": "saipul",
    "start_date": datetime(2021, 12, 10),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "msaipulrx@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def retrieve_data_customers():
    
    sql="SELECT * FROM customers where country = 'France'"
    select_table = PostgresHook(postgres_conn_id='postgresql_db')
    connection = select_table.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    sources = cursor.fetchall()
    for source in sources:
        print("Source : {0} - activated: {1}".format(source[0], source[1]))
    return sources

def retrieve_data_categories():
    
    sql_categories='SELECT * FROM region'
    select_table_cat = PostgresHook(postgres_conn_id='postgresql_db')
    connection_cat = select_table_cat.get_conn()
    cursor_cat = connection_cat.cursor()
    cursor_cat.execute(sql_categories)
    sources_cat = cursor_cat.fetchall()
    for source_cat in sources_cat:
        print("Source : {0} - activated: {1}".format(source_cat[0], source_cat[1]))
    return sources_cat

with DAG(dag_id="postgre_read_data", schedule_interval="@once", default_args=default_args, catchup=False) as dag:

    retrieve_data_cust = PythonOperator(
        task_id='selectTableCustomer',
        python_callable=retrieve_data_customers
    )

    retrieve_data_cat = PythonOperator(
        task_id='selectTableCategories',
        python_callable=retrieve_data_categories
    )

    retrieve_data_cust >> retrieve_data_cat
