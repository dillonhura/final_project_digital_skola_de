from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import requests
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'retries': 2,
}

dag = DAG('covid19_etl_pipeline',
          default_args=default_args,
          description='End-to-end ETL pipeline for COVID-19 data',
          schedule_interval='@daily',
          start_date=datetime(2024, 1, 1),
          catchup=False)

def extract_data(**kwargs):
    api_url = "http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian?level=kab"
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()['data']['content']
        return data
    else:
        raise ValueError(f"Failed to fetch data from API: {response.text}")

def load_data_to_mysql(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='extract_data_task')
    
    # Read the SQL script from the file
    with open('scripts/mysql/insert_api_daily_data.sql', 'r') as file:
        sql_script = file.read()
    
    # SQL script contains placeholders for parameters, 
    # replace them with actual values from the data
    insert_queries = []
    for record in data:
        # Replace placeholders in the SQL script with actual values
        sql_query = sql_script.format(
            column1=record['column1'], 
            column2=record['column2'], 
            # Add more placeholders and corresponding values as needed
        )
        
        # Construct parameters for the MySqlOperator
        parameters = {
            'parameter_name1': record['parameter_name1'],
            'parameter_name2': record['parameter_name2'],
            # Add more parameters as needed
        }
        
        # Create MySqlOperator
        mysql_insert_task = MySqlOperator(
            task_id=f'mysql_insert_task_{record["record_id"]}',
            sql=sql_query,
            parameters=parameters,
            mysql_conn_id='mysql_connection_id',  # Replace with your MySQL connection ID
            dag=dag
        )
        insert_queries.append(mysql_insert_task)
    
    return insert_queries
    return mysql_insert_task


def aggregate_district_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='extract_data_task')
    # Perform aggregation on district data
    # You may need to implement aggregation logic here
    pass

def load_aggregate_data_to_postgresql(**kwargs):
    aggregated_data = kwargs['ti'].xcom_pull(task_ids='aggregate_district_data_task')
    # Assuming you have a PostgreSQL connection configured in Airflow
    # You can adjust the parameters as needed
    for record in aggregated_data:
        # Insert aggregated data into PostgreSQL
        pass

# Tasks
extract_data_task = PythonOperator(
    task_id='extract_data_task',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

load_data_to_mysql_task = PythonOperator(
    task_id='load_data_to_mysql_task',
    python_callable=load_data_to_mysql,
    provide_context=True,
    dag=dag,
)

aggregate_district_data_task = PythonOperator(
    task_id='aggregate_district_data_task',
    python_callable=aggregate_district_data,
    provide_context=True,
    dag=dag,
)

load_aggregate_data_to_postgresql_task = PythonOperator(
    task_id='load_aggregate_data_to_postgresql_task',
    python_callable=load_aggregate_data_to_postgresql,
    provide_context=True,
    dag=dag,
)

# Dependencies
extract_data_task >> load_data_to_mysql_task
load_data_to_mysql_task >> aggregate_district_data_task
aggregate_district_data_task >> load_aggregate_data_to_postgresql_task
