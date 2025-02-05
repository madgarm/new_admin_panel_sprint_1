CREATE SCHEMA IF NOT EXISTS content;

CREATE TABLE IF NOT EXISTS content.film_work (
    id uuid PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT NULL,
    creation_date DATE NULL,
    rating FLOAT NULL,
    type VARCHAR(15) NOT NULL,
    created timestamp with time zone NOT NULL DEFAULT NOW(),
    modified timestamp with time zone NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS content.person (
    id uuid PRIMARY KEY,
    full_name TEXT NOT NULL,
    created timestamp with time zone NOT NULL DEFAULT NOW(),
    modified timestamp with time zone NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS content.genre (
    id uuid PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NULL,
    created timestamp with time zone NOT NULL DEFAULT NOW(),
    modified timestamp with time zone NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS content.person_film_work (
    id uuid PRIMARY KEY,
    film_work_id uuid NOT NULL REFERENCES content.film_work (id),
    person_id uuid NOT NULL REFERENCES content.person (id),
    role TEXT NOT NULL,
    created timestamp with time zone NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS content.genre_film_work (
    id uuid PRIMARY KEY,
    film_work_id uuid NOT NULL REFERENCES content.film_work (id),
    genre_id uuid NOT NULL REFERENCES content.genre (id),
    created timestamp with time zone NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS film_work_creation_date_idx ON content.film_work(creation_date);
CREATE INDEX IF NOT EXISTS film_work_person_film_work_idx ON content.person_film_work(film_work_id);
CREATE INDEX IF NOT EXISTS film_work_person_person_idx ON content.person_film_work(person_id);
CREATE UNIQUE INDEX IF NOT EXISTS film_work_genre_idx ON content.genre_film_work (film_work_id, genre_id);