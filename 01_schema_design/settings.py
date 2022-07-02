import logging
import os
from typing import Optional

from dotenv import load_dotenv
from pydantic import BaseSettings, validator

dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)


class Settings(BaseSettings):
    DB_NAME: str = os.environ['DB_NAME']
    DB_USER: str = os.environ['DB_USER']
    DB_PASSWORD: str = os.environ['DB_PASSWORD']
    DB_HOST: str = os.environ['DB_HOST']
    DB_PORT: int = os.environ.get('DB_PORT', 5432)
    DATABASE_URL: Optional[str] = os.environ.get('DATABASE_URL')
    DB_SCHEMA: str = os.environ.get('DB_SCHEMA', 'content')

    CONTENT_PERSONS_COUNT: int = os.environ.get('CONTENT_PERSONS_COUNT', 100000)
    CONTENT_GENRES_COUNT: int = os.environ.get('CONTENT_GENRES_COUNT', 15)
    CONTENT_FILM_WORK_COUNT: int = os.environ.get('CONTENT_FILM_WORK_COUNT', 1100000)
    CONTENT_PAGE_SIZE_COUNT: int = os.environ.get('CONTENT_PAGE_SIZE_COUNT', 1000)

    LOG_LEVEL: str = os.environ.get('LOG_LEVEL', 'INFO')

    @validator('DATABASE_URL', pre=True, always=True)
    def default_DATABASE_URL(cls, value, *, values, **kwargs):
        database_url = value or (
            f'postgresql://{values["DB_USER"]}:{values["DB_PASSWORD"]}@'
            f'{values["DB_HOST"]}:{values["DB_PORT"]}/{values["DB_NAME"]}'
        )
        return database_url


settings = Settings()

logging.basicConfig(level=settings.LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
