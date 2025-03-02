from pathlib import Path

# Project
CONFIG_DIR = "config"
DATA_DIR = "data"
ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = "scripts"

# API
CURRENT_API = "api-football"
REQUEST_LIMIT_PER_MINUTE = 400
SLEEP_TIME = 60 / REQUEST_LIMIT_PER_MINUTE

# GOOGLE DRIVE
GOOGLE_DRIVE_GRODT_FOLDER_ID = "123nk299C42r7XedkDfynqUMMJBdqKjm8"
