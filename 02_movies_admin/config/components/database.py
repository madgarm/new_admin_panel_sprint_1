import os

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': os.environ['DB_NAME'],
        'USER': os.environ['DB_USER'],
        'PASSWORD': os.environ['DB_PASSWORD'],
        'HOST': os.environ['DB_HOST'],
        'PORT': os.environ.get('DB_PORT', 5432),
        'OPTIONS': {'options': f'-c search_path={os.environ.get("DB_SCHEMA", "content")},public -c log_statement=mod'},
    }
}
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
