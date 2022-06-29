import datetime
import os
import uuid
import sqlite3
from contextlib import closing
from typing import List, Union

from dotenv import load_dotenv

import psycopg2
from psycopg2.extras import execute_batch
from pydantic.main import BaseModel

load_dotenv('./02_movies_admin/config/.env')


# Предвидя вопрос, почему не dataclasses - если стоит цель валидация данных, а не просто объявление интерфейса,
# куда удобней использовать например pydantic. С dataclasses пришлось бы колдовать с post_init и кастомными валидаторами
class Person(BaseModel):
    id: uuid.UUID
    full_name: str


class Genre(BaseModel):
    id: uuid.UUID
    name: str
    description: str = None


class FilmWork(BaseModel):
    id: uuid.UUID
    title: str
    description: str = None
    creation_date: datetime.datetime = None
    rating: float = None
    type: str


class GenreFilmWork(BaseModel):
    id: uuid.UUID
    film_work_id: uuid.UUID
    genre_id: uuid.UUID


class PersonFilmWork(BaseModel):
    id: uuid.UUID
    film_work_id: uuid.UUID
    person_id: uuid.UUID
    role: str


class PostgresService:

    def __init__(self, dsn: dict):
        self.dsn = dsn
        self.connection = None

    def __enter__(self):
        self.connection = psycopg2.connect(**self.dsn)
        self.connection.autocommit = False
        # чтобы postgresql мог работать с форматом UUID без костылей
        psycopg2.extras.register_uuid()
        return self

    def __exit__(self, *exc):
        if self.connection:
            self.connection.commit()
            self.connection.close()

    def save_all_data(self, data: List[Union[Person, Genre, FilmWork, GenreFilmWork, PersonFilmWork]], query: str):
        ready_data = [tuple(instance.dict().values()) for instance in data]
        try:
            cursor = self.connection.cursor()
            execute_batch(cur=cursor, sql=query, argslist=ready_data)
        except (Exception, psycopg2.DatabaseError, psycopg2.IntegrityError) as exc:
            print(exc)

    def get_count_for_table(self, table_name: str):
        query = f'SELECT COUNT(*) FROM {table_name}'
        with closing(self.connection.cursor()) as cursor:
            try:
                cursor.execute(query)
                return cursor.fetchone()
            except (Exception, psycopg2.DatabaseError) as exc:
                print(exc)

    def get_row(self, query: str):
        with closing(self.connection.cursor()) as cursor:
            try:
                cursor.execute(query)
                return cursor.fetchone()
            except (Exception, psycopg2.DatabaseError) as exc:
                print(exc)


class SQLiteService:

    def __init__(self, dsn: dict, size: int = 1000):
        self.dsn = dsn
        self.size = size
        self.connection = None

    def __enter__(self):
        self.connection = sqlite3.connect(**self.dsn)
        return self

    def __exit__(self, *exc):
        if self.connection:
            self.connection.commit()
            self.connection.close()

    def get_data(self, query: str):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            while row is not None:
                yield row
                row = cursor.fetchone()
            cursor.close()
        except Exception as exc:
            print(exc)

    def get_count_for_table(self, table_name: str):
        query = f'SELECT COUNT(*) FROM {table_name}'
        with closing(self.connection.cursor()) as cursor:
            try:
                cursor.execute(query)
                return cursor.fetchone()
            except (Exception, sqlite3.DatabaseError) as exc:
                print(exc)


def load_from_sqlite(postgres_dsn: dict, sqllite_dsn: dict, size: int):
    """
    Основной метод загрузки данных из SQLite в Postgres
    """

    tables_model_map = {
        'person': Person,
        'genre': Genre,
        'film_work': FilmWork,
        'genre_film_work': GenreFilmWork,
        'person_film_work': PersonFilmWork,
    }

    with SQLiteService(dsn=sqllite_dsn, size=size) as sqllite, PostgresService(dsn=postgres_dsn) as postgres:
        for table in tables_model_map:
            model = tables_model_map[table]

            # строка с полями целевой модели данных
            fields = ', '.join([field for field in model.__fields__])

            # запрос на выборку данных из sqllite
            query_to_get = f'SELECT {fields} FROM {table}'

            delimiters = ', '.join(['%s' for _ in range(len(model.__fields__))])
            # запрос на вставку данных в postgresql
            query_to_migrate = f'INSERT INTO content.{table} ({fields}) ' \
                               f'VALUES ({delimiters}) ON CONFLICT (id) DO NOTHING'

            data = list()

            for row in sqllite.get_data(query=query_to_get):
                checked_row = model(**{key: row[i] for i, key in enumerate(model.__fields__)})
                data.append(checked_row)
                if len(data) == size:
                    postgres.save_all_data(data=data, query=query_to_migrate)
                    data = list()

            if len(data):
                postgres.save_all_data(data=data, query=query_to_migrate)


if __name__ == '__main__':
    load_dotenv('../02_movies_admin/config/.env')
    postgres_dsn = {
        'dbname': os.environ.get('DB_NAME'),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'host': os.environ.get('DB_HOST'),
        'port': os.environ.get('DB_PORT'),
        'options': '-c search_path=content',
    }
    sqllite_dsn = {
        'database': os.environ.get('SQLLITE_FILENAME'),
    }
    # размер блока для записи
    migrate_data_size = int(os.environ.get('MIGRATE_DATA_SIZE'))
    load_from_sqlite(postgres_dsn=postgres_dsn, sqllite_dsn=sqllite_dsn, size=migrate_data_size)
