# Предвидя вопрос, почему не dataclasses - если стоит цель валидация данных, а не просто объявление интерфейса,
# куда удобней использовать например pydantic. С dataclasses пришлось бы колдовать с post_init и кастомными валидаторами
import datetime
import uuid

from pydantic.main import BaseModel


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
