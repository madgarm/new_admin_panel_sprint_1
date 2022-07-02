import logging
import os
from typing import Optional

from dotenv import load_dotenv
from pydantic import BaseSettings, validator

dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)


class Settings(BaseSettings):
    PG_DB_NAME: str = os.environ['DB_NAME']
    PG_DB_USER: str = os.environ['DB_USER']
    PG_DB_PASSWORD: str = os.environ['DB_PASSWORD']
    PG_DB_HOST: str = os.environ['DB_HOST']
    PG_DB_PORT: int = os.environ.get('DB_PORT', 5432)
    PG_DATABASE_URL: Optional[str] = os.environ.get('DATABASE_URL')
    PG_DB_SCHEMA: str = os.environ.get('DB_SCHEMA', 'content')

    SQLITE_FILENAME: str = os.environ['SQLITE_FILENAME']

    MIGRATE_DATA_SIZE: int = os.environ.get('MIGRATE_DATA_SIZE', 1000)
    LOG_LEVEL: str = os.environ.get('LOG_LEVEL', 'INFO')

    @validator('PG_DATABASE_URL', pre=True, always=True)
    def default_PG_DATABASE_URL(cls, value, *, values, **kwargs):
        database_url = value or (
            f'postgresql://{values["PG_DB_USER"]}:{values["PG_DB_PASSWORD"]}@'
            f'{values["PG_DB_HOST"]}:{values["PG_DB_PORT"]}/{values["PG_DB_NAME"]}'
        )
        return database_url


settings = Settings()

logging.basicConfig(level=settings.LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
