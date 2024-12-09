import logging

from typing import Any

from config.entity_names import PROCESSED_DIR
from config.vars import ROOT_DIR, DATA_DIR
from helpers.utils import move_json_files_between_directories
from services.json_processor import JsonProcessor

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class FixturesPipeline:
    def __init__(self, entity_config: dict[str, dict[str, Any]]):
        self.entity_config = entity_config

    def run_entity_pipeline(self, entity_name: str) -> None:
        logging.info(f"* * * * * RUNNING PIPELINE FOR {entity_name.upper()} * * * * *")
        config = self.entity_config[entity_name]

        # 1. Get dates to update
        logging.info("Getting dates to update...")
        dates_to_update = config["dates_to_update_method"]()

        # 2. Pull data from API
        logging.info("Pulling data...")
        config["api_pull_method"](dates_to_update)

        # 3. Parse data and upsert to db
        if config["multiprocessing"]:
            processor = JsonProcessor(
                entity=entity_name,
                parse_method=config["parse_method"],
                upsert_method=config["upsert_method"],
                input_dir=config["input_dir"],
                chunk_size=5,
            )
            processor.run_multiprocessing()
        else:
            df = config["parse_method"]()
            for dep in config["dependencies"]:

                dep(df)
            config["upsert_method"](df)

        # 4. Move files to "processed" directory
        move_json_files_between_directories(
            f"{ROOT_DIR}/{DATA_DIR}/{config['input_dir']}",
            f"{ROOT_DIR}/{DATA_DIR}/{config['input_dir']}/{PROCESSED_DIR}",
        )

    def run_daily_refresh(self, entities: list[str]) -> None:
        for entity in entities:
            self.run_entity_pipeline(entity)
