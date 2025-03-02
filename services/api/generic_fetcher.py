# generic_fetcher.py
import logging
import time
from typing import Callable, List, Optional

from config.vars import SLEEP_TIME
from services.api.api_fetcher import ApiFetcher


class GenericFetcher(ApiFetcher):
    """
    A single fetcher class that can handle various use cases:
      - Pulling data by date
      - Pulling data for a list of IDs (fixtures, teams, coaches, etc.)
      - Single or multiple endpoints
    Minimizes code duplication: pass relevant parameters for each scenario.
    """

    def pull_data_for_list(
        self,
        values: List[str],
        endpoint_template: str,
        filename_prefix: str,
        subdir: str,
        sleep_time: int = SLEEP_TIME,
        extra_params: dict = None,
        transform_value_func: Optional[Callable[[str], str]] = None,
    ) -> None:
        """
        Fetches data for each item in 'values' list, using a dynamic endpoint.
        Useful for fixtures, coaches, teams, etc.

        :param values: List of IDs, or countries, or other items to iterate over
        :param endpoint_template: e.g. "fixtures?date={}", "coaches?team={}" where '{}' is replaced by item
        :param filename_prefix: e.g. "FIXTURES_", "COACH_"
        :param subdir: e.g. "fixtures", "coaches", etc. for storing JSON
        :param sleep_time: seconds to sleep between requests (avoid rate limit)
        :param extra_params: additional query parameters (dict) appended to the URL
        :param transform_value_func: optional function to transform each item into a string
                                     for the endpoint (e.g. int -> str).
        """
        if not values:
            logging.info("No items to fetch. Exiting.")
            return

        extra_params = extra_params or {}

        for item in values:
            final_value = transform_value_func(item) if transform_value_func else item
            endpoint = endpoint_template.format(final_value)
            response = self.fetch_data(endpoint, **extra_params)
            if response:
                filename = f"{filename_prefix}{final_value}"
                self.write_response_to_json(response, filename, subdir)
            else:
                logging.info(f"No data for item: {item}")
            time.sleep(sleep_time)

    def pull_data_by_dates(
        self,
        dates: List[str],
        endpoint_template: str,
        filename_prefix: str,
        subdir: str,
        sleep_time: int = SLEEP_TIME,
        extra_params: dict = None,
    ) -> None:
        """
        Pulls data for a list of date strings, e.g. "2023-09-01".
        Similar to pull_data_for_list, but specialized for date-based endpoints.
        """
        self.pull_data_for_list(
            values=dates,
            endpoint_template=endpoint_template,
            filename_prefix=filename_prefix,
            subdir=subdir,
            sleep_time=sleep_time,
            extra_params=extra_params,
        )

    def pull_single_endpoint(self, endpoint: str, extra_params: dict = None) -> None:
        """
        If you just need to pull data from a single endpoint once.
        """
        filename = endpoint.upper()
        subdir = endpoint
        extra_params = extra_params or {}
        response = self.fetch_data(endpoint, **extra_params)
        if response:
            self.write_response_to_json(response, filename, subdir)
        else:
            logging.warning(f"No data returned for endpoint: {endpoint}")
