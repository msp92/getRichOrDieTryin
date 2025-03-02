import datetime as dt

from config.vars import (
    GOOGLE_API_CREDENTIALS,
    GOOGLE_DRIVE_GRODT_FOLDER_ID,
)
from models.analytics.breaks import BreaksTeamStats
from models.base import BaseMixin
from services import Db
from services.exporters.google_drive_exporter import GoogleDriveExporter

# NOTE: czy to powinno iść do pipelines/ ?


def main():
    db = Db()
    BaseMixin.set_db(db)
    exporter = GoogleDriveExporter(db.engine, GOOGLE_API_CREDENTIALS)

    # TODO: prepare config for all exports

    ### BREAKS
    file_name = f"breaks_{dt.datetime.now().strftime('%Y%m%d%H%M')}.xlsx"
    breaks_with_coaches_df = exporter.run_query_from_file("breaks_with_coaches.sql")
    breaks_team_stats_df = BreaksTeamStats.get_all()
    upcoming_fixtures_df = exporter.run_query_from_file(
        "upcoming_games_with_coaches.sql"
    )

    exporter.export_to_xlsx(
        [breaks_with_coaches_df, breaks_team_stats_df, upcoming_fixtures_df],
        ["breaks_with_coaches", "team_stats", "upcoming_games"],
        file_name,
    )
    exporter.upload_to_google_drive(file_name, GOOGLE_DRIVE_GRODT_FOLDER_ID)

    ## DETAILED FIXTURES
    file_name = f"detailed_fixtures_{dt.datetime.now().strftime('%Y%m%d%H%M')}.xlsx"
    detailed_fixtures_df = exporter.run_query_from_file(
        "fixtures_events_summary_new.sql"
    )

    exporter.export_to_xlsx(
        [detailed_fixtures_df],
        ["detailed_fixtures"],
        file_name,
    )
    exporter.upload_to_google_drive(file_name, GOOGLE_DRIVE_GRODT_FOLDER_ID)


if __name__ == "__main__":
    main()
