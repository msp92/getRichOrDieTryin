from data_processing.data_parsing import parse_fixtures
from models.fixtures import Fixture
from utils.utils import move_json_files_between_directories

### Update specific tables with newly pulled data ###

### Fixtures ###
df_updated_fixtures = parse_fixtures("updates")
Fixture.update(df_updated_fixtures)
move_json_files_between_directories("data/fixtures/updates", "data/fixtures/updates/processed")

### Fixture Stats ###

### Fixture Events ###

### Player Stats ###
