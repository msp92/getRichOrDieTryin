import logging

from config.entity_names import PROCESSED_DIR
from config.vars import ROOT_DIR, DATA_DIR
from helpers.utils import move_json_files_between_directories
from jobs.config.fixtures_config import ENTITY_CONFIG
from services.json_processor import JSONProcessor


class FixturesPipeline:
    def __init__(self, entity_config: dict):
        self.entity_config = entity_config

    def run_entity_pipeline(self, entity_name: str) -> None:
        logging.info(f"* * * * * RUNNING PIPELINE FOR {entity_name.upper()} * * * * *")
        config = self.entity_config[entity_name]

        # 1. Get dates to
        logging.info("Getting dates to update...")
        dates_to_update = config["dates_to_update_method"]()

        # 2. Pull data from API
        logging.info("Pulling data...")
        config["api_pull_method"](dates_to_update)

        # 3. Parse data and upsert to db
        if config["multiprocessing"]:
            processor = JSONProcessor(
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


if __name__ == "__main__":
    pipeline = FixturesPipeline(ENTITY_CONFIG)
    pipeline.run_daily_refresh(
        entities=["fixtures", "fixture_stats", "fixture_player_stats", "fixture_events"]
    )
