import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from data_ingest import ingest_callable 
from data_process import processor_callable
from data_output import output_callable

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

local_workflow = DAG(
    "LocalIngestionDag",
    schedule_interval = "0 0 * * 1",
    start_date = datetime(2023, 6, 1)
)

URL = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/geonames-all-cities-with-a-population-1000/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/cities_output_{{execution_date.strftime(\'%Y-%m-%d\')}}.csv'
TABLE_NAME_TEMPLATE = 'cities_{{execution_date.strftime(\'%Y-%m-%d\')}}'
LIMIT_POPULATION = 10000000
RESULT_FILE = AIRFLOW_HOME + '/result_countries_{{execution_date.strftime(\'%Y-%m-%d\')}}.csv'

with local_workflow:
    
    data_wget_task = BashOperator(
        task_id = 'wget',
        bash_command = f'wget -O {OUTPUT_FILE_TEMPLATE} {URL}'
        
    )
    
    data_ingest_task = PythonOperator(
        task_id='ingest',
        python_callable=ingest_callable,
        op_kwargs=dict(
            user = PG_USER,
            password = PG_PASSWORD,
            host = PG_HOST,
            port = PG_PORT,
            db = PG_DATABASE,
            table_name = TABLE_NAME_TEMPLATE,
            csv_file = OUTPUT_FILE_TEMPLATE
        ),
    )
    
    data_processing_task = PythonOperator(
        task_id='processor',
        python_callable=processor_callable,
        op_kwargs=dict(
            user = PG_USER,
            password = PG_PASSWORD,
            host = PG_HOST,
            port = PG_PORT,
            db = PG_DATABASE,
            table_name = TABLE_NAME_TEMPLATE,
            limit_num = LIMIT_POPULATION
        ),
    )
    
    data_output_task = PythonOperator(
        task_id='output',
        python_callable=output_callable,
        provide_context=True,
        op_kwargs=dict(
            result_file = RESULT_FILE
        )
    )
    
    
    
    data_wget_task >> data_ingest_task >> data_processing_task >> data_output_task








