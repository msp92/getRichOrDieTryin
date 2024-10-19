import logging

from data_processing.data_parsing import (
    parse_leagues,
    parse_seasons,
    parse_countries,
    parse_teams,
    parse_fixtures,
)
from models.analytics.breaks.breaks import Break  # NOQA: F401
from models.data.fixtures.fixtures import Fixture  # NOQA: F401
from models.data.main import League, Season, Team, Country
from services.db import Db

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

db = Db()

### Recreating all main tables with existing data ###

db.drop_all_tables()
db.create_all_tables()

### Countries ###
df_countries = parse_countries()
Country.upsert(df_countries)

### Leagues ###
df_leagues = parse_leagues()
League.upsert(df_leagues)

### Teams ###
df_teams = parse_teams()
Team.upsert(df_teams)

### Seasons ###
df_seasons = parse_seasons()
Season.upsert(df_seasons)

### Fixtures ###
df_fixtures = parse_fixtures("league_seasons")
Team.insert_missing_teams_into_db(df_fixtures)
Fixture.upsert(df_fixtures)
