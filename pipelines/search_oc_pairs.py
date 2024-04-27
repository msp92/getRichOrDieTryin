import logging
from models.fixtures import Fixture
from utils.utils import write_df_to_csv, search_overcome_pairs

### Search oc pairs ###

df_overcome_games = Fixture.get_overcome_games()
write_df_to_csv(df_overcome_games, "overcome_games")
logging.info(
    f"Overcome fixtures: {len(df_overcome_games)} out of {Fixture.count_all()} all fixtures."
)
search_overcome_pairs(df_overcome_games)