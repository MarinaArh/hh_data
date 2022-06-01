import datetime
import itertools

import requests
import numpy as np

import pandas as pd


API_URL = 'https://api.hh.ru'

BASE_PARAMS = {
    'specialization': '1', # информационные технологии
    'per_page': 100,
    'date_from': datetime.date.today() - datetime.timedelta(days=1),
    'date_to': datetime.date.today()
}

excluded_vacancies = [] # для проблемных вакансий

class SkipVacancy(Exception):
    ...


def get_areas():
    return requests.get(f'{API_URL}/areas').json()


def get_cities(area_id):
    return requests.get(f'{API_URL}/areas/{area_id}').json()['areas']


def get_page(area):
    '''Загрузка'''
    l = BASE_PARAMS['per_page']
    params = {**BASE_PARAMS, **{'area': area}}
    for page in range(100):
        if l < BASE_PARAMS['per_page']:
            break
        params['page'] = page
        resp = requests.get(f'{API_URL}/vacancies', params)
        if resp.status_code != 200:
            break
        data = resp.json()
        l = len(data['items'])
        yield [x['id'] for x in data['items']]


def get_vacancy(id_):
    '''Загрузка одной вакансии'''
    try:
        resp = requests.get(f'{API_URL}/vacancies/{id_}')
        if resp.status_code != 200:
            raise Exception(f"Load vacancy error, id = {id_}!")
        info = resp.json()
        df = pd.json_normalize(info)
        if 'salary' in df.columns:
            df = df[['id', 'description', 'key_skills', 'schedule.id', 'accept_handicapped',
                 'accept_kids', 'experience.id', 'employer.id', 'employer.name',
                 'response_letter_required', 'salary', 'name', 'area.name', 'created_at',
                 'published_at', 'employment.id']]
            df.rename(columns={'salary': 'salary.from'}, inplace = True)
            df['salary.from'] = np.nan
            df.insert(11, 'salary.to', np.nan)
            df.insert(12, 'salary.gross', np.nan)
            df.insert(13, 'salary.currency', np.nan)
        else:
            df = df[['id', 'description', 'key_skills', 'schedule.id', 'accept_handicapped',
                 'accept_kids', 'experience.id', 'employer.id', 'employer.name',
                 'response_letter_required', 'salary.from', 'salary.to', 'salary.gross',
                 'salary.currency', 'name', 'area.name', 'created_at', 'published_at',
                 'employment.id']]
        employer_id = df['employer.id'][0]
        employer = requests.get(f'{API_URL}/employers/{employer_id}').json()
        df.insert(9, 'employer_industries', '\n'.join([x['name'] for x in employer['industries']]))
        key_skills = '\n'.join([x['name'] for x in itertools.chain(*df['key_skills'])])
        df['key_skills'] = np.nan if key_skills == '' else key_skills
        return df
    except Exception:
        raise SkipVacancy()
