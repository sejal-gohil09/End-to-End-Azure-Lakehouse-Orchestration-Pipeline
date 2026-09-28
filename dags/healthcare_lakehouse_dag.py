from datetime import datetime, timedelta
from airflow import DAG

default_args = {'owner': 'data_engineering', 'start_date': datetime(2026, 1, 1)}
dag = DAG('owid_healthcare_pipeline', default_args=default_args, schedule_interval='@daily', catchup=False)
