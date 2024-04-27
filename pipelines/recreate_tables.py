from data_processing.data_parsing import parse_countries, parse_leagues, parse_teams, parse_seasons, parse_fixtures, \
    parse_events, parse_fixture_stats, parse_fixture_player_stats
from models.countries import Country
from models.fixtures import Fixture
from models.fixtures_events import FixtureEvent
from models.fixtures_players_stats import FixturePlayerStat
from models.fixtures_stats import FixtureStat
from models.leagues import League
from models.seasons import Season
from models.teams import Team
from services.db import drop_all_tables, create_all_tables
from utils.utils import insert_missing_teams_into_db, move_json_files_between_directories

### Recreating all main tables with existing data ###

drop_all_tables()
create_all_tables()

### Countries ###
df_countries = parse_countries()
Country.insert_df(df_countries)

### Leagues ###
df_leagues = parse_leagues()
League.insert_df(df_leagues)

### Teams ###
df_teams = parse_teams()
Team.insert_df(df_teams)

### Seasons ###
df_seasons = parse_seasons()
Season.insert_df(df_seasons)

## Fixtures ###
df_fixtures = parse_fixtures("league_seasons")
insert_missing_teams_into_db(df_fixtures)
Fixture.insert_df(df_fixtures)


### Fixture Events, Stats, Player Stats ###
df_fixture_events = parse_events()
FixtureEvent.insert_df(df_fixture_events)
df_fixture_stats = parse_fixture_stats()
FixtureStat.insert_df(df_fixture_stats)
df_fixture_player_stats = parse_fixture_player_stats()
FixturePlayerStat.insert_df(df_fixture_player_stats)