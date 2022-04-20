from django.contrib import admin
from django.db.models import Prefetch
from django.utils.translation import gettext_lazy as _

from movies.models import Genre, FilmWork, GenreFilmWork, Person, PersonFilmWork


class GenreFilmWorkInline(admin.TabularInline):
    model = GenreFilmWork
    autocomplete_fields = ('genre', )
    list_prefetch_related = (Prefetch('film_work'), Prefetch('genre'))
    verbose_name = _('genre of filmwork')
    verbose_name_plural = _('genres of filmwork')
    extra = 2


class PersonFilmWorkInline(admin.TabularInline):
    model = PersonFilmWork
    autocomplete_fields = ('person', )
    list_prefetch_related = (Prefetch('film_work'), Prefetch('person'))
    verbose_name = _('person in filmwork')
    verbose_name_plural = _('persons in filmwork')
    extra = 2


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    search_fields = ('name', 'description')
    list_display = ('name', 'description', 'created', 'modified')
    list_filter = ()
    readonly_fields = ('created', 'modified')

    def film_works(self, instance):
        return instance.film_works


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    search_fields = ('full_name', )
    list_display = ('full_name', 'created', 'modified')
    list_filter = ()
    readonly_fields = ('created', 'modified')

    def film_works(self, instance):
        return instance.film_works


@admin.register(FilmWork)
class FilmWorkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmWorkInline, PersonFilmWorkInline)
    list_display = ('title', 'type', 'creation_date', 'rating', 'created', 'modified')
    list_filter = ('type', 'genre')
    search_fields = ('title', 'description', 'id')
    readonly_fields = ('created', 'modified')
