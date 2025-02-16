import csv
import logging
from time import sleep

from config.api_config import ApiConfig
from config.vars import SLEEP_TIME, DATA_DIR, ROOT_DIR
from services.fetchers.events_fetcher import EventsFetcher
from services.fetchers.playerstats_fetcher import PlayerStatsFetcher
from services.fetchers.stats_fetcher import StatsFetcher

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# TODO: think of adding below methods to corresponding fetcher classes

api_config = ApiConfig()
stats_fetcher = StatsFetcher(config=api_config)
player_stats_fetcher = PlayerStatsFetcher(config=api_config)
events_fetcher = EventsFetcher(config=api_config)

# with open(
#     f"{ROOT_DIR}/{DATA_DIR}/TO_PULL/fixtures_stats.csv",
#     mode="r",
#     encoding="utf-8",
# ) as csvfile:
#     reader = csv.reader(csvfile)
#     for fixture_id in reader:
#         endpoint = f"fixtures/statistics?fixture={fixture_id[0]}"
#         resp = stats_fetcher.fetch_data(endpoint)
#         stats_fetcher.write_response_to_json(
#             resp, f"STATS_{fixture_id[0]}", "fixture_stats"
#         )
#         sleep(SLEEP_TIME)

with open(
    f"{ROOT_DIR}/{DATA_DIR}/TO_PULL/player_stats.csv",
    mode="r",
    encoding="utf-8",
) as csvfile:
    reader = csv.reader(csvfile)
    for fixture_id in reader:
        endpoint = f"fixtures/players?fixture={fixture_id[0]}"
        resp = player_stats_fetcher.fetch_data(endpoint)
        player_stats_fetcher.write_response_to_json(
            resp, f"PLAYER_STATS_{fixture_id[0]}", "fixture_player_stats"
        )
        sleep(SLEEP_TIME)

with open(
    f"{ROOT_DIR}/{DATA_DIR}/TO_PULL/events.csv",
    mode="r",
    encoding="utf-8",
) as csvfile:
    reader = csv.reader(csvfile)
    for fixture_id in reader:
        endpoint = f"fixtures/events?fixture={fixture_id[0]}"
        resp = events_fetcher.fetch_data(endpoint)
        events_fetcher.write_response_to_json(resp, f"EVENTS_{fixture_id[0]}", "fixture_events")
        sleep(SLEEP_TIME)

# with open(
#     f"{ROOT_DIR}/{DATA_DIR}/TO_PULL/lineups_to_pull_4.csv",
#     mode="r",
#     encoding="utf-8",
# ) as csvfile:
#     reader = csv.reader(csvfile)
#     for fixture_id in reader:
#         endpoint = f"fixtures/lineups?fixture={fixture_id[0]}"
#         resp = events_fetcher.fetch_data(endpoint)
#         events_fetcher.write_response_to_json(
#             resp, f"LINEUPS_{fixture_id[0]}", "fixture_lineups"
#         )
#         sleep(SLEEP_TIME)

# with open(
#     f"{ROOT_DIR}/{DATA_DIR}/TO_PULL/odds_20241111_20241125.csv",
#     mode="r",
#     encoding="utf-8",
# ) as csvfile:
#     reader = csv.reader(csvfile)
#     for fixture_id in reader:
#         endpoint = f"odds?fixture={fixture_id[0]}"
#         resp = events_fetcher.fetch_data(endpoint)
#         events_fetcher.write_response_to_json(
#             resp, f"ODDS_{fixture_id[0]}", "fixture_odds"
#         )
#         sleep(SLEEP_TIME)

# with open(
#     f"{ROOT_DIR}/{DATA_DIR}/TO_PULL/predictions_20240801_20241125.csv",
#     mode="r",
#     encoding="utf-8",
# ) as csvfile:
#     reader = csv.reader(csvfile)
#     for fixture_id in reader:
#         endpoint = f"predictions?fixture={fixture_id[0]}"
#         resp = events_fetcher.fetch_data(endpoint)
#         events_fetcher.write_response_to_json(
#             resp, f"PREDICTIONS_{fixture_id[0]}", "fixture_predictions"
#         )
#         sleep(SLEEP_TIME)
