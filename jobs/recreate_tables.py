import logging

from data_processing.data_parsing import (
    parse_coaches,
)
from models.analytics.breaks.breaks import Break  # NOQA: F401
from models.data.fixtures.fixtures import Fixture  # NOQA: F401
from models.data.main.coaches import Coach
from services.db import Db

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

db = Db()

### Coaches ###
df_coaches = parse_coaches()
Coach.upsert(df_coaches)
