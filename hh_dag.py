import csv
import time
import datetime
import hashlib
import pathlib
import itertools
import logging

import requests
import pandas as pd
import numpy as np

from airflow.models import DAG
from airflow.decorators import task

from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago


import headhunter as hh


logger = logging.getLogger("HH_DAG")

COLS = [
    'id', 'description', 'key_skills', 'schedule_id', 'accept_handicapped',
    'accept_kids', 'experience_id', 'employer_id', 'employer_name',
    'employer_industries', 'response_letter_required', 'salary_from',
    'salary_to', 'salary_gross', 'salary_currency', 'name', 'area_name',
    'created_at', 'published_at', 'employment_id'
]

SIZE = 100 # количество пачек вакансий

DESTINATION_DIR = "/home/airflow/export"

DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2022, 2, 28),
    'retries': 1,
    'depends_on_past': False,
}

DATE = hh.BASE_PARAMS["date_from"].strftime("%Y-%m-%d")


with DAG(
    'hh_dag',
    default_args=DEFAULT_ARGS,
    description='ETL from hh.ru',
    schedule_interval=None,
    tags=['vacancies']
) as dag:

    dag.doc_md = """
    Загрузка IT вакансий с hh.ru за сутки
    """

    with TaskGroup(group_id="crowler") as tg:

        @task()
        def get_countries():
            return [area['id'] for area in hh.get_areas()]

        @task()
        def load_ids_by_area(area_id: str):
            lst = []
            for x in hh.get_page(area_id):
                lst += x
            if len(lst) >= 2000: # достигли максимума загрузки
                # разобъем по городам
                lst = []
                for area in hh.get_cities(area_id):
                    for x in hh.get_page(area['id']):
                        lst += x
            return lst

        @task()
        def rearrange(ids_):
            ids_ = list(np.array(ids_).flatten())
            out = [
                ids_[i * SIZE:(i + 1) * SIZE]
                for i in range(len(ids_) // SIZE + 1)
            ]
            return next(iter(out))

        @task()
        def get_vacancies(ids_):
            '''Загрузка пачки вакансий'''

            skipped = []
            batch = pd.DataFrame(columns=COLS)

            for id_ in ids_:
                try:
                    df = hh.get_vacancy(id_)
                    if df is not None:
                        df.columns = COLS
                        batch = pd.concat([batch, df])
                except hh.SkipVacancy:
                    skipped.append(id_)
                except Exception:
                    pass

            hash_ = hashlib.sha256(str(ids_).encode()).hexdigest()
            filename = f"{DESTINATION_DIR}/{DATE}_{hash_}.csv"
            batch.to_csv(filename, mode='w', index=False)

        ids = load_ids_by_area.expand(area_id=get_countries())
        get_vacancies.expand(ids_=rearrange(ids))

    @task()
    def combine():
        path = pathlib.Path(DESTINATION_DIR)
        with (path / pathlib.Path(f"{DATE}.csv")).open(mode="w") as outfile:
            for file in path.glob(f"{DATE}_*.csv"):
                with file.open() as infile:
                    for line in infile:
                        outfile.write(line)
                file.unlink()

    tg >> combine()
