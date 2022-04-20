from dotenv import load_dotenv
from split_settings.tools import include

load_dotenv()

include(
    'components/app.py',
    'components/database.py',
    'components/localization.py',
    'components/security.py',
    'components/static.py',
    'components/time.py',
)
