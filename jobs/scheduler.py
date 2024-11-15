import logging
import os
import subprocess
import sys

import schedule
import time

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

DAILY_SCHEDULE_DATE = "00:00"
WEEKLY_SCHEDULE_DATE = "01:00"


def run_job(job_name: str) -> None:
    logging.info(f"Running {job_name} job...")
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


schedule.every().day.at(DAILY_SCHEDULE_DATE).do(run_job("daily_refresh"))
schedule.every().day.at(WEEKLY_SCHEDULE_DATE).do(run_job("weekly_refresh"))

while True:
    schedule.run_pending()
    time.sleep(1)
