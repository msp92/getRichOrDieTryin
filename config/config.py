# Project
SOURCE_DIR = "data"

# PostgreSQL
DB_HOST = "localhost"
DB_PORT = 5432
DB_USER = "maciek"
DB_PASSWORD = "Torcik2024"
DB_NAME = "dev_football_archive"
#DB_CHARSET = "charset=utf8mb4"

# Database connection parameters
DB_PARAMS = {
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "host": DB_HOST,
    "port": DB_PORT,
    #"charset": "charset=utf8mb4"
}
DB_URL = (f'postgresql://{DB_PARAMS["user"]}:{DB_PARAMS["password"]}'
          f'@{DB_PARAMS["host"]}:{DB_PARAMS["port"]}'
          f'/{DB_PARAMS["dbname"]}')

# API
API_BASE_URL = "https://v3.football.api-sports.io"
API_HEADER_KEY_NAME = "x-apisports-key"
API_HEADER_KEY_VALUE = "3c25fe383eb82ed188e24a4f9ae24947"
API_HEADER_HOST_NAME = "x-rapidapi-host"
API_HEADER_HOST_VALUE = "v3.football.api-sports.io"
