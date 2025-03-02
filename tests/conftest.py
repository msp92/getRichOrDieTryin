import os
import pytest
from dotenv import load_dotenv

from config.vars import ROOT_DIR

env_file = ".env.rds" if os.getenv("DOCKER_ENV") else ".env.local"
env_path = os.path.join(ROOT_DIR, env_file)
load_dotenv(dotenv_path=env_path)


@pytest.fixture(scope="session")
def db_config():
    """Provide database configuration from environment variables."""
    return {
        "dbname": os.getenv("DB_NAME", ""),
        "user": os.getenv("DB_USER", ""),
        "password": os.getenv("DB_PASSWORD", ""),
        "host": os.getenv("DB_HOST", ""),
        "port": os.getenv("DB_PORT", ""),
    }
