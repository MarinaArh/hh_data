import pandas as pd
import pyarrow as pa
import datetime

from airflow.models import DAG
from airflow.decorators import task

from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2022, 2, 28),
    'retries': 3,
    'depends_on_past': False,
}

DESTINATION_DIR = "/home/airflow/export"

def convert_to_parquet():
	dyear = (datetime.date.today() - datetime.timedelta(days=1)).year
	df = pd.read_csv(f'{DESTINATION_DIR}/{str(dyear)}.csv')
	table = pa.Table.from_pandas(df)
	pa.parquet.write_table(table, f'{DESTINATION_DIR}/{str(dyear)}.parquet')


with DAG(
    'hh_to_parquet_dag',
    default_args=DEFAULT_ARGS,
    description='Monthly convert csv to parquet',
    schedule_interval='0 12 1 * *',
    tags=['vacancies']
) as dag:

    dag.doc_md = """
    Ежемесячно формирует parquet за текущий год с учетом прошедшего месяца
    """

    start_task = DummyOperator(
        task_id='start'
    )

    convert_task = PythonOperator(
        task_id='convert',
        python_callable=convert_to_parquet,
    )

    start_task >> convert_task



