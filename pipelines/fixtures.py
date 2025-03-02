import logging
import sys
from typing import Any

import pandas as pd

from data_processing.data_processing import load_json_file_names_from_directory
from data_processing.json_processor import JsonProcessor
from pipelines.base import BasePipeline
from pipelines.config import FIXTURE_ENTITIES_CONFIG

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class FixturesPipeline(BasePipeline):
    def __init__(self, entity_config: dict[str, dict[str, Any]]):
        super().__init__(entity_config)

    def fetch(self) -> None:
        """Fetch data from API and save it to JSON files."""
        config = self.config
        if "dates_to_update_method" in config and config["dates_to_update_method"]:
            dates = config["dates_to_update_method"]()
            logging.info(f"Fetched dates for {self._current_entity_name}: {dates}")
        else:
            dates = None

        if "api_pull_method" in config and config["api_pull_method"]:
            logging.info(f"Calling api_pull_method for {self._current_entity_name}")
            try:
                config["api_pull_method"](dates)
            except Exception as e:
                logging.error(
                    f"Error fetching data for {self._current_entity_name}: {e}"
                )

    def process(self) -> None:
        """Process files or data for the current entity."""
        config = self.config
        try:
            file_paths = load_json_file_names_from_directory(self._current_entity_name)
            logging.info(
                f"Loaded {len(file_paths)} files for {self._current_entity_name}"
            )
        except Exception as e:
            logging.error(f"Error loading files for {self._current_entity_name}: {e}")
            return

        if config.get("multiprocessing", False):
            processor = JsonProcessor(
                parse_method=config.get("parse_method"), batch_size=100, num_processes=2
            )
            self._current_df = processor.process_files(file_paths)
        else:
            dataframes = [config["parse_method"](fp) for fp in file_paths]
            self._current_df = pd.concat(dataframes) if dataframes else pd.DataFrame()

            # Apply dependencies in both modes if they exist
        if "dependencies" in config and config["dependencies"]:
            for dep in config["dependencies"]:
                logging.info(
                    f"Applying dependency {dep.__name__} for {self._current_entity_name}"
                )
                dep(self._current_df)

    def load(self) -> None:
        """Upsert processed data to the database."""
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
    pipeline = FixturesPipeline(FIXTURE_ENTITIES_CONFIG)
    method = sys.argv[1] if len(sys.argv) > 1 else "run"
    pipeline.run(method=method)
