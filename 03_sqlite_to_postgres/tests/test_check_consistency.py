import os

import pytest
from dotenv import load_dotenv
from load_data import FilmWork, Genre, GenreFilmWork, Person, PersonFilmWork, PostgresService, SQLiteService


@pytest.fixture(autouse=True)
def load_env():
    dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '.env')
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)


@pytest.fixture
def postgres_db(load_env):
    postgres_dsn = (
        f'postgresql://{os.environ["DB_USER"]}:{os.environ["DB_PASSWORD"]}@'
        f'{os.environ["DB_HOST"]}:{os.environ.get("DB_PORT", 5432)}/{os.environ["DB_NAME"]}'
    )
    return PostgresService(dsn=postgres_dsn)


@pytest.fixture
def sqllite_db():
    sqllite_db = f'../{os.environ["SQLITE_FILENAME"]}'
    return SQLiteService(database=sqllite_db)


@pytest.fixture
def db_table_model_map():
    table_model_map = {
        'person': Person,
        'genre': Genre,
        'film_work': FilmWork,
        'genre_film_work': GenreFilmWork,
        'person_film_work': PersonFilmWork,
    }
    return table_model_map


@pytest.mark.database_access
def test_consistency_between_tables_total(postgres_db, sqllite_db, db_table_model_map):
    """
    Тест соответствия количества записей в каждой из таблиц между исходной и целевой базами
    """

    with sqllite_db, postgres_db:
        for table in db_table_model_map:
            sqllite_count = sqllite_db.get_count_for_table(table_name=table)
            postgres_count = postgres_db.get_count_for_table(table_name=table)
            assert sqllite_count == postgres_count


@pytest.mark.database_access
def test_consistency_between_tables_detail(postgres_db, sqllite_db, db_table_model_map):
    """
    Тест соответствия каждой записи в каждой из таблиц между исходной и целевой базами
    """

    with sqllite_db, postgres_db:
        for table in db_table_model_map:
            model = db_table_model_map[table]

            fields = ', '.join([field for field in model.__fields__])
            query_to_get_sqllite = f'SELECT {fields} FROM {table}'

            for sqllite_row in sqllite_db.get_data(query=query_to_get_sqllite):
                sqllite_row_id = sqllite_row[0]
                query_to_get_postgres = f"SELECT {fields} FROM {table} WHERE id = '{sqllite_row_id}'"
                postgres_row = postgres_db.get_row(query=query_to_get_postgres)

                sqllite_checked_row = model(**{key: sqllite_row[i] for i, key in enumerate(model.__fields__)})
                postgresql_checked_row = model(**{key: postgres_row[i] for i, key in enumerate(model.__fields__)})

                assert sqllite_checked_row == postgresql_checked_row
