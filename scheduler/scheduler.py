import datetime as dt
import logging
import os
import subprocess
import sys
import schedule
import time

from typing import List, Dict


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Define scheduled times
DAILY_SCHEDULE_DATES = ["21:12"]

# List of pipelines to schedule
PIPELINES = {
    "main": {
        "entities": ["leagues", "seasons", "teams"],
        "separate_archives": False,  # Treat all data together
    },
    # "fixtures": {
    #     "entities": [
    #         "fixtures",
    #         "fixture_stats",
    #         "fixture_player_stats",
    #         "fixture_events",
    #     ],
    #     "separate_archives": True,  # Each entity needs to be packed separately
    # },
    # "analytics_breaks": {
    #     "entities": ["breaks", "breaks_team_stats"],
    #     "separate_archives": True,  # Each entity needs separate processing
    # },
}

MAX_RETRIES = 3


def run_job(job_name: str, action: str = "") -> None:
    """Execute a pipeline job for a specific action (fetch, process, load) with retry logic.

    Args:
        job_name (str): Name of the pipeline job to run (e.g., 'data_sync').
        action (str, optional): Specific action to perform (e.g., 'fetch'). Defaults to "".
    """
    logging.info(f"* * * * RUNNING {job_name.upper()} {action.upper()} JOB * * * *")

    # Full path to project root
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    MAX_RETRIES = 3

    # Check if running in Docker (e.g., on EC2)
    if os.getenv("DOCKER_ENV") == "1":
        # Running inside container, execute directly
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                cmd = [sys.executable, "-m", f"pipelines.{job_name}", action]
                subprocess.run(cmd, check=True, cwd=project_root)
                logging.info(f"Job {job_name} {action} completed successfully")
                return
            except subprocess.CalledProcessError as e:
                logging.error(f"Attempt {attempt} failed for {job_name} {action}: {e}")
                if attempt == MAX_RETRIES:
                    logging.error(
                        f"Job {job_name} {action} failed after {MAX_RETRIES} attempts"
                    )
    else:
        # Local run (Windows or Linux without Docker)
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                if sys.platform == "win32":
                    venv_activate = os.path.join(
                        project_root, ".venv", "Scripts", "activate.bat"
                    )
                    cmd = (
                        f'"{venv_activate}" && python -m pipelines.{job_name} {action}'
                    )
                    subprocess.run(cmd, shell=True, check=True, cwd=project_root)
                else:
                    venv_python = os.path.join(project_root, ".venv", "bin", "python")
                    cmd = [venv_python, "-m", f"pipelines.{job_name}", action]
                    subprocess.run(cmd, check=True, cwd=project_root)

                logging.info(f"Job {job_name} {action} completed successfully")
                return
            except subprocess.CalledProcessError as e:
                logging.error(f"Attempt {attempt} failed for {job_name} {action}: {e}")
                if attempt == MAX_RETRIES:
                    logging.error(
                        f"Job {job_name} {action} failed after {MAX_RETRIES} attempts"
                    )


def schedule_pipelines(schedule_dates: List[str], pipelines: Dict[str, Dict]) -> None:
    """Schedules multiple pipelines with actions at specified times."""
    for date in schedule_dates:
        for pipeline, config in pipelines.items():
            schedule.every().day.at(date).do(run_job, job_name=pipeline)
            # TODO: enable run for all given actions?
            # schedule.every().day.at(date).do(run_job, job_name=pipeline, action="fetch")
            # schedule.every().day.at(date).do(
            #     run_job, job_name=pipeline, action="process"
            # )
            # schedule.every().day.at(date).do(run_job, job_name=pipeline, action="load")


def get_next_run_time(schedule_dates: List[str]) -> dt.datetime:
    """Finds the next scheduled run time."""
    now = dt.datetime.now()
    next_times = []
    for date in schedule_dates:
        scheduled_time = dt.datetime.strptime(date, "%H:%M").replace(
            year=now.year, month=now.month, day=now.day
        )
        if scheduled_time < now:
            scheduled_time += dt.timedelta(days=1)
        next_times.append(scheduled_time)
    return min(next_times)


if __name__ == "__main__":
    logging.info("* * * * PIPELINE SCHEDULER * * * *")
    logging.info(f"Jobs will run at: {DAILY_SCHEDULE_DATES}")

    # db_instance = Db()
    # BaseMixin.set_db(db_instance)

    schedule_pipelines(DAILY_SCHEDULE_DATES, PIPELINES)

    while True:
        next_run = get_next_run_time(DAILY_SCHEDULE_DATES)
        while dt.datetime.now() < next_run:
            schedule.run_pending()
            time.sleep(60)
