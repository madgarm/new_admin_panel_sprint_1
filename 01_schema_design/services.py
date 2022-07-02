import logging
from contextlib import closing
from typing import List

import psycopg2
from settings import settings

logger = logging.getLogger()


class PostgresService:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.connection = None

    def __enter__(self):
        self.connection = psycopg2.connect(dsn=self.dsn, **{'options': f'-c search_path={settings.DB_SCHEMA}'})
        self.connection.autocommit = False
        psycopg2.extras.register_uuid()
        logger.debug(f'Установлено соединение с базой данных с параметрами {self.dsn}')
        return self

    def __exit__(self, *exc):
        if self.connection:
            self.connection.commit()
            self.connection.close()

    def save_all_data(self, data: List[tuple], query: str):
        with closing(self.connection.cursor()) as cursor:
            try:
                psycopg2.extras.execute_batch(cur=cursor, sql=query, argslist=data)
            except (psycopg2.IntegrityError, psycopg2.DatabaseError) as exc:
                logger.exception(exc)

    def get_data(self, query: str):
        with closing(self.connection.cursor()) as cursor:
            try:
                cursor.execute(query)
                row = cursor.fetchone()
                while row is not None:
                    yield row
                    row = cursor.fetchone()
            except psycopg2.DatabaseError as exc:
                logger.exception(exc)

    def get_count_for_table(self, table_name: str):
        query = f'SELECT COUNT(*) FROM {table_name}'
        with closing(self.connection.cursor()) as cursor:
            try:
                cursor.execute(query)
                return cursor.fetchone()
            except psycopg2.DatabaseError as exc:
                logger.exception(exc)

    def get_row(self, query: str):
        with closing(self.connection.cursor()) as cursor:
            try:
                cursor.execute(query)
                return cursor.fetchone()
            except psycopg2.DatabaseError as exc:
                logger.exception(exc)
