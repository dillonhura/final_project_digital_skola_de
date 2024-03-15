from datetime import datetime, timedelta
from airflow import DAG
from airflow.configuration import AirflowConfigParser
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import requests
import pandas as pd


# Initialize the AirflowConfigParser
config_parser = AirflowConfigParser()

# Get the MySQL connection ID from the configuration file
mysql_conn_id = config_parser.get('connections', 'mysql_default')

# Get the Postgres connection ID from the configuration file
postgres_conn_id = config_parser.get('connections', 'postgres_default')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def generate_insert_query(
        data, sql_template_file='scripts/mysql/insert_api_daily_template.sql'):
    # Read the SQL template from the file
    with open(sql_template_file, 'r') as file:
        sql_template = file.read()

    # List to store VALUES part of the query
    values = []

    # Iterate over data and construct the VALUES part
    for record in data:
        values.append(
            f"({record['CLOSECONTACT']}, "
            f"{record['CONFIRMATION']}, "
            f"{record['PROBABLE']}, "
            f"{record['SUSPECT']}, "
            f"{record['closecontact_dikarantina']}, "
            f"{record['closecontact_discarded']}, "
            f"{record['closecontact_meninggal']}, "
            f"{record['confirmation_meninggal']}, "
            f"{record['confirmation_sembuh']}, "
            f"{int(record['kode_kab'])}, "
            f"{int(record['kode_prov'])}, "
            f"'{record['nama_kab']}', "
            f"'{record['nama_prov']}', "
            f"{record['probable_diisolasi']}, "
            f"{record['probable_discarded']}, "
            f"{record['probable_meninggal']}, "
            f"{record['suspect_diisolasi']}, "
            f"{record['suspect_discarded']}, "
            f"{record['suspect_meninggal']}, "
            f"'{record['tanggal']}')"
        )

    # Combine the VALUES part into the SQL query
    sql_query = sql_template + ',\n'.join(values)
    
    return sql_query


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
    insert_query = generate_insert_query(data)

    # Create MySqlOperator
    mysql_insert_batch_task = MySqlOperator(
        task_id=f'mysql_insert_batch_task',
        sql=insert_query,
        mysql_conn_id=mysql_conn_id,
        dag=dag
    )

    return mysql_insert_batch_task


def aggregate_mysql_data(task_id, sql_file_path, **kwargs):
    with open(sql_file_path, 'r') as file:
        sql_query = file.read()

    mysql_task = MySqlOperator(
        task_id=task_id,
        mysql_conn_id=mysql_conn_id,
        sql=sql_query,  # SQL query read from the file
        dag=dag
    )
    mysql_task.execute(context=kwargs)


def insert_to_postgres(task_ids, target_table, **kwargs):
    # Pulling data from XCom
    mysql_result = kwargs['ti'].xcom_pull(task_ids=task_ids)
    
    # Convert MySQL result to DataFrame
    df = pd.DataFrame(mysql_result)
    
    # Insert data into PostgreSQL table
    df.to_sql(target_table, postgres_conn_id, if_exists='append', index=False)


# Extract and Dump Tasks
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

# District-Level Tasks
agg_district_monthly_task = PythonOperator(
    task_id='agg_district_monthly_task',
    python_callable=aggregate_mysql_data,
    op_kwargs={
        'task_id': 'agg_district_monthly',
        'sql_file_path': 'scripts/mysql/district/agg_monthly.sql',
    },
    dag=dag,
)

load_agg_district_monthly_to_postgresql_task = PythonOperator(
    task_id='load_agg_district_monthly_to_postgresql_task',
    python_callable=insert_to_postgres,
    op_kwargs={
        'task_ids': 'load_agg_district_monthly_to_postgresql',
        'target_table': 'DistrictMonthly',
    },
    dag=dag,
)

agg_district_yearly_task = PythonOperator(
    task_id='agg_district_yearly_task',
    python_callable=aggregate_mysql_data,
    op_kwargs={
        'task_id': 'agg_district_yearly',
        'sql_file_path': 'scripts/mysql/district/agg_yearly.sql',
    },
    dag=dag,
)

load_agg_district_yearly_to_postgresql_task = PythonOperator(
    task_id='load_agg_district_yearly_to_postgresql_task',
    python_callable=insert_to_postgres,
    op_kwargs={
        'task_ids': 'load_agg_district_yearly_to_postgresql',
        'target_table': 'DistrictYearly',
    },
    dag=dag,
)

# Province-Level Tasks
agg_province_daily_task = PythonOperator(
    task_id='agg_province_daily_task',
    python_callable=aggregate_mysql_data,
    op_kwargs={
        'task_id': 'agg_province_daily',
        'sql_file_path': 'scripts/mysql/province/agg_daily.sql',
    },
    dag=dag,
)

load_agg_province_daily_to_postgresql_task = PythonOperator(
    task_id='load_agg_province_daily_to_postgresql_task',
    python_callable=insert_to_postgres,
    op_kwargs={
        'task_ids': 'load_agg_province_daily_to_postgresql',
        'target_table': 'ProvinceDaily',
    },
    dag=dag,
)

agg_province_monthly_task = PythonOperator(
    task_id='agg_province_monthly_task',
    python_callable=aggregate_mysql_data,
    op_kwargs={
        'task_id': 'agg_province_monthly',
        'sql_file_path': 'scripts/mysql/province/agg_monthly.sql',
    },
    dag=dag,
)

load_agg_province_monthly_to_postgresql_task = PythonOperator(
    task_id='load_agg_province_monthly_to_postgresql_task',
    python_callable=insert_to_postgres,
    op_kwargs={
        'task_ids': 'load_agg_province_monthly_to_postgresql',
        'target_table': 'ProvinceMonthly',
    },
    dag=dag,
)

agg_province_yearly_task = PythonOperator(
    task_id='agg_province_yearly_task',
    python_callable=aggregate_mysql_data,
    op_kwargs={
        'task_id': 'agg_province_yearly',
        'sql_file_path': 'scripts/mysql/province/agg_yearly.sql',
    },
    dag=dag,
)

load_agg_province_yearly_to_postgresql_task = PythonOperator(
    task_id='load_agg_province_yearly_to_postgresql_task',
    python_callable=insert_to_postgres,
    op_kwargs={
        'task_ids': 'load_agg_province_yearly_to_postgresql',
        'target_table': 'ProvinceYearly',
    },
    dag=dag,
)

# Extract and Dump Task
extract_data_task >> load_data_to_mysql_task
# District aggregate
agg_district_monthly_task >> load_agg_district_monthly_to_postgresql_task
agg_district_yearly_task >> load_agg_district_yearly_to_postgresql_task
# Province aggregate
agg_province_daily_task >> load_agg_province_daily_to_postgresql_task
agg_province_monthly_task >> load_agg_province_monthly_to_postgresql_task
agg_province_yearly_task >> load_agg_province_yearly_to_postgresql_task
