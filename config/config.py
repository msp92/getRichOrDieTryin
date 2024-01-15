# Project
SOURCE_DIR = "data"

# PostgreSQL
DB_HOST = "localhost"
DB_PORT = 5433
DB_USER = "maciek"
DB_PASSWORD = "Torcik2024"
DB_NAME = "dev_football_archive"

# Database connection parameters
DB_PARAMS = {
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "host": DB_HOST,
    "port": DB_PORT
}
DB_URL = (f'postgresql://{DB_PARAMS["user"]}:{DB_PARAMS["password"]}'
          f'@{DB_PARAMS["host"]}:{DB_PARAMS["port"]}'
          f'/{DB_PARAMS["dbname"]}')

# API
API_BASE_URL = "https://api-football-v1.p.rapidapi.com/v3"
# API_BASE_URL_V1? = "https://v3.football.api-sports.io"
# API_BASE_URL_V1? = "https://api-football-v1.p.rapidapi.com/v3"
API_HEADER_KEY_NAME = "x-rapidapi-key"
API_HEADER_KEY_VALUE = "90d4cdeff9msh2c0dd24b0170b5ep157563jsnb5a4131306e2"
API_HEADER_HOST_NAME = "x-rapidapi-host"
API_HEADER_HOST_VALUE = "api-football-v1.p.rapidapi.com"
