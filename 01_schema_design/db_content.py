import os
import random
import uuid
from datetime import date, datetime

import psycopg2
from dotenv import load_dotenv
from faker import Faker
from psycopg2.extras import execute_batch

"""
Скрипт для заполнения базы данными
"""

load_dotenv('./02_movies_admin/config/.env')
fake = Faker()

dsn = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT'),
    'options': '-c search_path=content',
}

PERSONS_COUNT = 100000
GENRES_COUNT = 15
FILM_WORK_COUNT = 1100000
PAGE_SIZE = 10000
now = datetime.utcnow()


def generate_data():
    with psycopg2.connect(**dsn) as conn, conn.cursor() as cur:

        # Заполнение таблицы Person
        persons_ids = [str(uuid.uuid4()) for _ in range(PERSONS_COUNT)]
        query = 'INSERT INTO person (id, full_name, created, modified) VALUES (%s, %s, %s, %s)'
        data = [(pk, fake.name(), now, now) for pk in persons_ids]
        print('Сгенерировали данные для Person')
        execute_batch(cur, query, data, page_size=PAGE_SIZE)
        conn.commit()
        print('Записали данные в таблицу Person')

        # Заполнение таблицы Genre
        genre_ids = [str(uuid.uuid4()) for _ in range(GENRES_COUNT)]
        query = 'INSERT INTO genre (id, name, description, created, modified) VALUES (%s, %s, %s, %s, %s)'
        data = [(pk, fake.word(), fake.texts(), now, now) for pk in genre_ids]
        print('Сгенерировали данные для Genre')
        execute_batch(cur, query, data, page_size=PAGE_SIZE)
        conn.commit()
        print('Записали данные в таблицу Person')

        # Заполнение таблицы FilmWork
        film_work_ids = [str(uuid.uuid4()) for _ in range(FILM_WORK_COUNT)]
        query = (
            'INSERT INTO film_work (id, title, description, creation_date, rating, type, created, modified) '
            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
        )
        data = [
            (
                pk,
                fake.catch_phrase(),
                fake.texts(),
                fake.date_between_dates(date(year=1900, month=1, day=1)),
                fake.pyfloat(min_value=0, max_value=100, right_digits=1),
                fake.random_element(elements=('movie', 'tv_show')),
                now,
                now,
            )
            for pk in film_work_ids
        ]
        print('Сгенерировали данные для FilmWork')
        execute_batch(cur, query, data, page_size=PAGE_SIZE)
        conn.commit()
        print('Записали данные в таблицу FilmWork')

        # Заполнение таблицы PersonFilmWork и GenreFilmWork
        person_film_work_data = []
        genre_film_work_data = []

        roles = ['actor', 'producer', 'director']

        cur.execute('SELECT id FROM film_work')
        film_works_ids = [data[0] for data in cur.fetchall()]

        for film_work_id in film_works_ids:
            for person_id in random.sample(persons_ids, 5):
                role = random.choice(roles)
                person_film_work_data.append((str(uuid.uuid4()), film_work_id, person_id, role, now))

            for genre_id in random.sample(genre_ids, 3):
                genre_film_work_data.append((str(uuid.uuid4()), film_work_id, genre_id, now))

        print('Сгенерировали данные для PersonFilmWork и GenreFilmWork')

        query = 'INSERT INTO person_film_work (id, film_work_id, person_id, role, created) VALUES (%s, %s, %s, %s, %s)'
        execute_batch(cur, query, person_film_work_data, page_size=PAGE_SIZE)
        conn.commit()
        print('Записали данные в таблицу PersonFilmWork')

        query = 'INSERT INTO genre_film_work (id, film_work_id, genre_id, created) VALUES (%s, %s, %s, %s)'
        execute_batch(cur, query, genre_film_work_data, page_size=PAGE_SIZE)
        conn.commit()
        print('Записали данные в таблицу GenreFilmWork')
        print('Готово')


if __name__ == '__main__':
    generate_data()
