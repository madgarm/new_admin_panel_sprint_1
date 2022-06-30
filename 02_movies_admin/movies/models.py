import datetime
import uuid

from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.utils.translation import gettext_lazy as _
from movies.constants import FilmWorkType


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class CreatedMixin(models.Model):
    created = models.DateTimeField(_('date and time of creating'), default=datetime.datetime.utcnow)

    class Meta:
        abstract = True


class ModifiedMixin(models.Model):
    modified = models.DateTimeField(_('date and time of modifying'), default=datetime.datetime.utcnow)

    class Meta:
        abstract = True


class Genre(UUIDMixin, CreatedMixin, ModifiedMixin):
    name = models.TextField(_('name'))
    description = models.TextField(_('description'), null=True, blank=True)

    def __str__(self):
        return self.name

    class Meta:
        db_table = "content\".\"genre"
        verbose_name = _('genre')
        verbose_name_plural = _('genres')


class Person(UUIDMixin, CreatedMixin, ModifiedMixin):
    full_name = models.TextField(_('full name'))

    def __str__(self):
        return self.full_name

    class Meta:
        db_table = "content\".\"person"
        verbose_name = _('person')
        verbose_name_plural = _('persons')


class PersonFilmWork(UUIDMixin, CreatedMixin):
    film_work = models.ForeignKey('movies.FilmWork', on_delete=models.CASCADE, db_index=True)
    person = models.ForeignKey('movies.Person', on_delete=models.CASCADE, db_index=True)
    role = models.TextField(_('role'))

    class Meta:
        db_table = "content\".\"person_film_work"
        unique_together = ('film_work', 'person')


class GenreFilmWork(UUIDMixin, CreatedMixin):
    film_work = models.ForeignKey('movies.FilmWork', on_delete=models.CASCADE, db_index=False)
    genre = models.ForeignKey('movies.Genre', on_delete=models.CASCADE, db_index=False)

    class Meta:
        db_table = "content\".\"genre_film_work"
        unique_together = ('film_work', 'genre')
        indexes = (models.Index(fields=['film_work', 'genre']),)


class FilmWork(UUIDMixin, CreatedMixin, ModifiedMixin):
    title = models.TextField(_('title'), max_length=255)
    description = models.TextField(_('description'), null=True, blank=True)
    creation_date = models.DateField(_('creation date'), null=True, blank=True)
    rating = models.FloatField(
        _('rating'), null=True, blank=True, validators=[MinValueValidator(0), MaxValueValidator(100)]
    )
    type = models.CharField(_('type'), max_length=15, choices=FilmWorkType.choices)
    genre = models.ManyToManyField('movies.Genre', through='movies.GenreFilmWork')
    person = models.ManyToManyField('movies.Person', through='movies.PersonFilmWork')

    def __str__(self):
        return self.title

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = _('filmwork')
        verbose_name_plural = _('filmworks')
        indexes = (models.Index(fields=['creation_date']),)
