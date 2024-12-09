import logging

from jobs.config.analytics_breaks_config import ANALYTICS_BREAKS_ENTITIES_CONFIG

from jobs.analytics_breaks_pipeline import AnalyticsBreaksPipeline
from models.base import BaseMixin, Base
from services import Db

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    db_instance = Db()
    BaseMixin.set_db(db_instance)
    Base.metadata.create_all(db_instance.engine)
    try:
        pipeline = AnalyticsBreaksPipeline(ANALYTICS_BREAKS_ENTITIES_CONFIG)
        pipeline.run_daily_refresh(
            entities=[
                "breaks",
                "breaks_team_stats",
                "breaks_team_stats_share",
            ]
        )
    finally:
        db_instance.close()


if __name__ == "__main__":
    main()
