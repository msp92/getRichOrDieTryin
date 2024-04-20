# Project
SOURCE_DIR = "data"

# PostgreSQL
DB_HOST = "localhost"
DB_PORT = 5432
DB_USER = "maciek"
DB_PASSWORD = "Torcik2024"
DB_NAME = "dev_football_archive"

# Database connection parameters
DB_PARAMS = {
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "host": DB_HOST,
    "port": DB_PORT,
}
DB_URL = (f'postgresql://{DB_PARAMS["user"]}:{DB_PARAMS["password"]}'
          f'@{DB_PARAMS["host"]}:{DB_PARAMS["port"]}'
          f'/{DB_PARAMS["dbname"]}')

CURRENT_API = "api-football"

# API-FOOTBALL
API_FOOTBALL_BASE_URL = "https://v3.football.api-sports.io"
API_FOOTBALL_HEADER_KEY_NAME = "x-apisports-key"
API_FOOTBALL_HEADER_KEY_VALUE = "3c25fe383eb82ed188e24a4f9ae24947"
API_FOOTBALL_HEADER_HOST_NAME = "x-rapidapi-host"
API_FOOTBALL_HEADER_HOST_VALUE = "v3.football.api-sports.io"

# RAPID-API
RAPID_API_BASE_URL = "https://api-football-v1.p.rapidapi.com/v3"
RAPID_API_HEADER_KEY_NAME = "X-RapidAPI-Key"
RAPID_API_HEADER_KEY_VALUE = "90d4cdeff9msh2c0dd24b0170b5ep157563jsnb5a4131306e2"
RAPID_API_HEADER_HOST_NAME = "X-RapidAPI-Host"
RAPID_API_HEADER_HOST_VALUE = "api-football-v1.p.rapidapi.com"
