import logging
import sys
from typing import Dict, Any
import pandas as pd
from pipelines.base import BasePipeline
from pipelines.config import ANALYTICS_BREAKS_ENTITIES_CONFIG

logging.basicConfig(level=logging.INFO)


class AnalyticsBreaksPipeline(BasePipeline):
    def __init__(self, entity_config: Dict[str, Dict[str, Any]]):
        """Initialize the AnalyticsBreaksPipeline with entity configurations."""
        super().__init__(entity_config)

    def fetch(self) -> None:
        """Fetch data using the get_method from config."""
        config = self.config
        if "get_method" in config and config["get_method"]:
            logging.info(f"Fetching data for {self._current_entity_name}")
            try:
                self._current_df = config["get_method"]()
                logging.info(
                    f"Fetched {len(self._current_df)} records for {self._current_entity_name}"
                )
            except Exception as e:
                logging.error(
                    f"Error fetching data for {self._current_entity_name}: {e}"
                )
                self._current_df = pd.DataFrame()

    def process(self) -> None:
        """Process data using dependencies from config."""
        config = self.config
        if self._current_df.empty:
            logging.warning(f"No data to process for {self._current_entity_name}")
            return

        if "dependencies" in config and config["dependencies"]:
            for dep in config["dependencies"]:
                try:
                    logging.info(f"Applying dependency for {self._current_entity_name}")
                    # Jeśli dependency to callable (funkcja)
                    if callable(dep):
                        self._current_df = dep(self._current_df)
                    # Jeśli dependency to skrypt SQL (string)
                    elif isinstance(dep, str):
                        logging.info(f"Executing SQL script: {dep}")
                        # Tu możesz dodać logikę do uruchamiania SQL, np. przez pandas lub silnik bazy
                        # Przykład: self._current_df = pd.read_sql(dep, some_db_engine)
                    else:
                        logging.warning(f"Unknown dependency type for {dep}")
                except Exception as e:
                    logging.error(
                        f"Error applying dependency for {self._current_entity_name}: {e}"
                    )

    def load(self) -> None:
        """Upsert processed data to the database using upsert_method from config."""
        config = self.config
        df = self._current_df
        if "upsert_method" in config and config["upsert_method"] and not df.empty:
            logging.info(f"Upserting data for {self._current_entity_name} to DB...")
            try:
                config["upsert_method"](df)
            except Exception as e:
                logging.error(
                    f"Error upserting data for {self._current_entity_name}: {e}"
                )
        else:
            logging.warning(
                f"No upsert_method or empty DataFrame for {self._current_entity_name}, skipping load."
            )


if __name__ == "__main__":
    pipeline = AnalyticsBreaksPipeline(ANALYTICS_BREAKS_ENTITIES_CONFIG)
    method = sys.argv[1] if len(sys.argv) > 1 else "run"
    pipeline.run(method=method)
