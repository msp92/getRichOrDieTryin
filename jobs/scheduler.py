import logging
import os
import subprocess
import sys

import schedule
import time

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

DAILY_SCHEDULE_DATE = "23:00"


def run_job(job_name: str) -> None:
    logging.info(f"* * * * RUNNING {job_name.upper()} JOB * * * *")
    script_path = os.path.join(os.path.dirname(__file__), f"{job_name}.py")
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


logging.info("* * * * SCHEDULER * * * *")
logging.info(f"Next job will run at {DAILY_SCHEDULE_DATE}")
schedule.every().day.at(DAILY_SCHEDULE_DATE).do(lambda: run_job("main_weekly_update"))
schedule.every().day.at(DAILY_SCHEDULE_DATE).do(
    lambda: run_job("fixtures_daily_update")
)
schedule.every().day.at(DAILY_SCHEDULE_DATE).do(
    lambda: run_job("analytics_breaks_daily_update")
)
schedule.every().day.at(DAILY_SCHEDULE_DATE).do(lambda: run_job("export_daily_update"))


while True:
    schedule.run_pending()
    time.sleep(1)
