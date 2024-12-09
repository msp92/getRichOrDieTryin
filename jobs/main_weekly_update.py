from jobs.config.main_config import MAIN_ENTITIES_CONFIG
from jobs.main_pipeline import MainPipeline
from models.base import Base, BaseMixin
from services.db import Db


def main() -> None:
    db_instance = Db()
    BaseMixin.set_db(db_instance)
    Base.metadata.create_all(db_instance.engine)
    try:
        pipeline = MainPipeline(MAIN_ENTITIES_CONFIG)
        pipeline.run_daily_refresh(
            entities=["countries", "leagues", "seasons", "teams"]
        )
    finally:
        db_instance.close()


if __name__ == "__main__":
    main()
