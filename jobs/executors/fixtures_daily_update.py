from jobs.config.fixtures_config import FIXTURE_ENTITIES_CONFIG
from jobs.pipelines.fixtures_pipeline import FixturesPipeline
from models.base import Base, BaseMixin
from services.db import Db


def main() -> None:
    db_instance = Db()
    BaseMixin.set_db(db_instance)
    Base.metadata.create_all(db_instance.engine)
    try:
        pipeline = FixturesPipeline(FIXTURE_ENTITIES_CONFIG)
        pipeline.run_daily_refresh(
            entities=[
                # TODO: turn off manual referee mapping before the vacation !!!!!
                # TODO: execute weekly update first each day
                "fixtures",
                "fixture_stats",
                "fixture_player_stats",
                "fixture_events",
            ]
        )
    finally:
        db_instance.close()


if __name__ == "__main__":
    main()
