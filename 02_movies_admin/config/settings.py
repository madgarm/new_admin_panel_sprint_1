import os

from dotenv import load_dotenv
from split_settings.tools import include

dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

include(
    'components/app.py',
    'components/database.py',
    'components/localization.py',
    'components/security.py',
    'components/static.py',
    'components/time.py',
)
