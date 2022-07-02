import logging
import random
import uuid
from datetime import date

from faker import Faker
from services import PostgresService
from settings import settings

"""
Скрипт для заполнения базы данными
"""

logger = logging.getLogger()
fake = Faker()
PERSON_FILM_WORK_ROLES = ['actor', 'producer', 'director']


def generate_person_data():
    for _ in range(settings.CONTENT_PERSONS_COUNT):
        yield str(uuid.uuid4()), fake.name()


def generate_genre_data():
    for _ in range(settings.CONTENT_GENRES_COUNT):
        yield str(uuid.uuid4()), fake.word(), fake.texts()


def generate_film_work_data():
    for _ in range(settings.CONTENT_FILM_WORK_COUNT):
        yield (
            str(uuid.uuid4()),
            fake.catch_phrase(),
            fake.texts(),
            fake.date_between_dates(date(year=1900, month=1, day=1)),
            fake.pyfloat(min_value=0, max_value=100, right_digits=1),
            fake.random_element(elements=('movie', 'tv_show')),
        )


def generate_person_film_work_data(film_work_id: uuid.UUID, person_id: uuid.UUID):
    yield str(uuid.uuid4()), film_work_id, person_id, random.choice(PERSON_FILM_WORK_ROLES)


def generate_genre_film_work_data(film_work_id: uuid.UUID, genre_id: uuid.UUID):
    yield str(uuid.uuid4()), film_work_id, genre_id


def filling_table_with_generated_data(pg_service: PostgresService, table: str):
    filling_parameters = {
        'person': {
            'query': 'INSERT INTO person (id, full_name) VALUES (%s, %s) ON CONFLICT DO NOTHING',
            'function': generate_person_data,
        },
        'genre': {
            'query': 'INSERT INTO genre (id, name, description) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING',
            'function': generate_genre_data,
        },
        'film_work': {
            'query': 'INSERT INTO film_work (id, title, description, creation_date, rating, type) '
            'VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING',
            'function': generate_film_work_data,
        },
    }

    logger.info(f'Запись данных в таблицу {table} начата')
    data = list()
    for row in filling_parameters[table]['function']():
        data.append(row)
        if (saved_rows := len(data)) == settings.CONTENT_PAGE_SIZE_COUNT:
            pg_service.save_all_data(data=data, query=filling_parameters[table]['query'])
            logger.debug(f'В таблицу {table} записано {saved_rows} записей')
            data = list()
    else:
        if saved_rows := len(data):
            pg_service.save_all_data(data=data, query=filling_parameters[table]['query'])
            logger.debug(f'В таблицу {table} записано {saved_rows} записей')
    logger.info(f'Запись данных в таблицу {table} завершена')


def filling_intermediate_table_with_generated_data(
    pg_service: PostgresService, intermediate_table: str, related_table: str, root_table: str = 'film_work'
):
    filling_parameters = {
        'person_film_work': {
            'query': 'INSERT INTO person_film_work (id, film_work_id, person_id, role) '
            'VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING',
            'function': generate_person_film_work_data,
        },
        'genre_film_work': {
            'query': 'INSERT INTO genre_film_work (id, film_work_id, genre_id) '
            'VALUES (%s, %s, %s) ON CONFLICT DO NOTHING',
            'function': generate_genre_film_work_data,
        },
    }

    get_root_obj_id_query = f'SELECT id FROM {root_table}'
    get_related_obj_id_query = f'SELECT id FROM {related_table} ORDER BY RANDOM() LIMIT 1'

    logger.info(f'Запись данных в таблицу {intermediate_table} начата')
    data = list()
    related_obj_id = None
    for root_obj_id in pg_service.get_data(query=get_root_obj_id_query):
        if not related_obj_id:
            related_obj_id = pg_service.get_row(query=get_related_obj_id_query)
        person_film_work = next(filling_parameters[intermediate_table]['function'](root_obj_id, related_obj_id))
        data.append(person_film_work)

        if (saved_rows := len(data)) == settings.CONTENT_PAGE_SIZE_COUNT:
            pg_service.save_all_data(data=data, query=filling_parameters[intermediate_table]['query'])
            logger.debug(f'В таблицу {intermediate_table} записано {saved_rows} записей')
            data = list()
            related_obj_id = None
    else:
        if saved_rows := len(data):
            pg_service.save_all_data(data=data, query=filling_parameters[intermediate_table]['query'])
            logger.debug(f'В таблицу {intermediate_table} записано {saved_rows} записей')
    logger.info(f'Запись данных в таблицу {intermediate_table} завершена')


def generate_data():
    simple_tables = (
        'person',
        'genre',
        'film_work',
    )

    intermediate_tables = {
        'person_film_work': 'person',
        'genre_film_work': 'genre',
    }

    with PostgresService(dsn=settings.DATABASE_URL) as postgres:

        for table in simple_tables:
            filling_table_with_generated_data(pg_service=postgres, table=table)

        for table in intermediate_tables:
            filling_intermediate_table_with_generated_data(
                pg_service=postgres, intermediate_table=table, related_table=intermediate_tables[table]
            )


if __name__ == '__main__':
    logger.info('Работа скрипта начата')
    generate_data()
    logger.info('Работа скрипта завершена')
