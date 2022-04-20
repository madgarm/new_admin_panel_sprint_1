from django.db import models


class FilmWorkType(models.TextChoices):
    MOVIE = 'movie', 'Фильм'
    TV_SHOW = 'tv_show', 'ТВ-шоу'
