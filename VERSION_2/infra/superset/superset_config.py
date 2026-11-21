import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "superset_secret_key_32_chars_1234")

PG_USER = os.environ.get("PG_USER", "dota")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "")
PG_DB = os.environ.get("SUPERSET_DB", "superset")

SQLALCHEMY_DATABASE_URI = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@postgres:5432/{PG_DB}"

WTF_CSRF_ENABLED = True
WTF_CSRF_EXEMPT_LIST = []
WTF_CSRF_TIME_LIMIT = None

ENABLE_PROXY_FIX = True

CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}

GUEST_ROLE_NAME = "Gamma"
