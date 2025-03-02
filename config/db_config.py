from dotenv import load_dotenv
import os

env_file = ".env.rds" if os.getenv("DOCKER_ENV") else ".env.local"
load_dotenv(env_file)


class DbConfig:
    def __init__(self) -> None:
        self.DB_NAME = os.getenv("DB_NAME", "")
        self.DB_USER = os.getenv("DB_USER", "")
        self.DB_PASSWORD = os.getenv("DB_PASSWORD", "")
        self.DB_HOST = os.getenv("DB_HOST", "")
        self.DB_PORT = os.getenv(
            "DB_PORT",
        )
