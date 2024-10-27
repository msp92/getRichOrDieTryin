import logging

from models.analytics.breaks import Pair
from models.data.fixtures.fixtures import Fixture
from helpers.utils import write_df_to_csv

### Search oc pairs ###

breaks_df = Fixture.get_breaks()
write_df_to_csv(breaks_df, "breaks")
logging.info(f"Total breaks: {len(breaks_df)} out of {Fixture.count_all()} all games.")
Pair.search_overcome_pairs(breaks_df)
