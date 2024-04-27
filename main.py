import logging
from services.api import check_subscription_status
from services.db import check_db_connection

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def main():
    check_db_connection()
    check_subscription_status()  # Doesn't work in Rapid API


if __name__ == "__main__":
    main()
