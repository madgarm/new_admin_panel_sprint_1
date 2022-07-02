import logging

from schemas import FilmWork, Genre, GenreFilmWork, Person, PersonFilmWork
from services import PostgresService, SQLiteService
from settings import settings

"""
Скрипт для миграции данных из SQLite в PostgreSQL
"""

logger = logging.getLogger()


def load_from_sqlite(postgres_dsn: str, sqllite_db: str, size: int):
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

    with SQLiteService(database=sqllite_db, size=size) as sqllite, PostgresService(dsn=postgres_dsn) as postgres:
        for table in tables_model_map:
            model = tables_model_map[table]

            # строка с полями целевой модели данных
            fields = ', '.join([field for field in model.__fields__])

            # запрос на выборку данных из sqllite
            query_to_get = f'SELECT {fields} FROM {table}'

            delimiters = ', '.join(['%s' for _ in range(len(model.__fields__))])
            # запрос на вставку данных в postgresql
            query_to_migrate = (
                f'INSERT INTO content.{table} ({fields}) ' f'VALUES ({delimiters}) ON CONFLICT (id) DO NOTHING'
            )

            data = list()
            logger.info(f'Перенос данных таблицы {table} начат')
            for row in sqllite.get_data(query=query_to_get):
                checked_row = model(**{key: row[i] for i, key in enumerate(model.__fields__)})
                data.append(checked_row)
                if (saved_rows := len(data)) == size:
                    postgres.save_all_data(data=data, query=query_to_migrate)
                    logger.debug(f'Для таблицы {table} обработано {saved_rows} записей')
                    data = list()
            else:
                if saved_rows := len(data):
                    postgres.save_all_data(data=data, query=query_to_migrate)
                    logger.debug(f'Для таблицы {table} обработано {saved_rows} записей')
            logger.info(f'Перенос данных таблицы {table} завершен')


if __name__ == '__main__':
    logger.info('Работа скрипта начата')
    load_from_sqlite(
        postgres_dsn=settings.PG_DATABASE_URL, sqllite_db=settings.SQLITE_FILENAME, size=settings.MIGRATE_DATA_SIZE
    )
    logger.info('Работа скрипта завершена')
