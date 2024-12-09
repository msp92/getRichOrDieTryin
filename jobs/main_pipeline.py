import logging

from typing import Any


class MainPipeline:
    def __init__(self, entity_config: dict[str, dict[str, Any]]):
        self.entity_config = entity_config

    def run_entity_pipeline(self, entity_name: str) -> None:
        logging.info(f"* * * * * RUNNING PIPELINE FOR {entity_name.upper()} * * * * *")
        config = self.entity_config[entity_name]

        # 1. Pull data from API
        if config["api_pull_method"]:
            logging.info("Pulling data...")
            config["api_pull_method"]()

        # 2. Parse data
        df = config["parse_method"]()

        # 3. Upsert data to db
        config["upsert_method"](df)

    def run_daily_refresh(self, entities: list[str]) -> None:
        for entity in entities:
            self.run_entity_pipeline(entity)
