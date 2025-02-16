import datetime as dt
import logging
import os
import subprocess
import sys

import schedule
import time

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

DAILY_SCHEDULE_DATES = ["06:00", "20:30", "23:00"]


def run_job(job_name: str) -> None:
    logging.info(f"* * * * RUNNING {job_name.upper()} JOB * * * *")
    script_path = os.path.join(os.path.dirname(__file__), "executors", f"{job_name}.py")
    venv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".venv")

    if sys.platform == "win32":
        activate_script = os.path.join(venv_path, "Scripts", "activate.bat")
        subprocess.run(f"{activate_script} && python {script_path}", shell=True)
    else:
        activate_script = os.path.join(venv_path, "bin", "activate")
        subprocess.run(
            f"source {activate_script} && python {script_path}",
            shell=True,
            executable="/bin/bash",
        )


def get_next_run_time(schedule_dates):
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
    logging.info("* * * * SCHEDULER * * * *")
    logging.info(f"Jobs will run at: {DAILY_SCHEDULE_DATES}")

    for date in DAILY_SCHEDULE_DATES:
        schedule.every().day.at(date).do(run_job, job_name="main_weekly_update")
        schedule.every().day.at(date).do(run_job, job_name="fixtures_daily_update")
        schedule.every().day.at(date).do(run_job, job_name="analytics_breaks_daily_update")
        schedule.every().day.at(date).do(run_job, job_name="export_daily_update")

    while True:
        next_run = get_next_run_time(DAILY_SCHEDULE_DATES)

        while dt.datetime.now() < next_run:
            schedule.run_pending()
            time.sleep(60)
