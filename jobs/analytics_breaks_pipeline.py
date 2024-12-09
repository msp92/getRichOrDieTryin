import logging

from typing import Any

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class AnalyticsBreaksPipeline:
    def __init__(self, entity_config: dict[str, dict[str, Any]]):
        self.entity_config = entity_config

    def run_entity_pipeline(self, entity_name: str) -> None:
        logging.info(f"* * * * * RUNNING PIPELINE FOR {entity_name.upper()} * * * * *")
        config = self.entity_config[entity_name]

        # 1. Get data
        logging.info("Getting data to build analytics table...")
        data_df = config["get_method"]()

        # 2. Perform aggregations
        if config["dependencies"]:
            final_data_df = config["dependencies"][0](data_df)
        else:
            final_data_df = data_df

        # 3. Parse data and upsert to db
        config["upsert_method"](final_data_df)

    def run_daily_refresh(self, entities: list[str]) -> None:
        for entity in entities:
            self.run_entity_pipeline(entity)
