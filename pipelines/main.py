import logging
import sys
from typing import Any

import pandas as pd

from pipelines.config import MAIN_ENTITIES_CONFIG
from pipelines.base import BasePipeline


class MainPipeline(BasePipeline):
    def __init__(self, entity_config: dict[str, dict[str, Any]]):
        super().__init__(entity_config)

    def fetch(self) -> None:
        config = self.config

        if "api_pull_method" in config and config["api_pull_method"]:
            logging.info(f"Fetching data for {self._current_entity_name}")
            config["api_pull_method"]()

        self._current_df = pd.DataFrame()

    def process(self) -> None:
        config = self.config
        df = self._current_df

        if "parse_method" in config and config["parse_method"]:
            logging.info(f"Parsing data for {self._current_entity_name}")
            df = config["parse_method"]()

        self._current_df = df

    def load(self) -> None:
        config = self.config
        df = self._current_df

        if "upsert_method" in config and config["upsert_method"]:
            logging.info(f"Upserting data for {self._current_entity_name}")
            config["upsert_method"](df)
        else:
            logging.warning(f"No upsert method for {self._current_entity_name}")


if __name__ == "__main__":
    pipeline = MainPipeline(MAIN_ENTITIES_CONFIG)
    method = sys.argv[1] if len(sys.argv) > 1 else "run"
    pipeline.run(method=method)
