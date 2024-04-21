import json
import os
from datetime import datetime
from time import sleep
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from config.config import (
    CURRENT_API,
    API_FOOTBALL_BASE_URL,
    API_FOOTBALL_HEADER_KEY_NAME,
    API_FOOTBALL_HEADER_KEY_VALUE,
    API_FOOTBALL_HEADER_HOST_NAME,
    API_FOOTBALL_HEADER_HOST_VALUE,
    RAPID_API_BASE_URL,
    RAPID_API_HEADER_KEY_NAME,
    RAPID_API_HEADER_KEY_VALUE,
    RAPID_API_HEADER_HOST_NAME,
    RAPID_API_HEADER_HOST_VALUE, SOURCE_DIR,
)
import requests
from models.countries import Country
from models.fixtures import Fixture
from models.leagues import League
from models.seasons import Season
from services.db import get_engine

if CURRENT_API == "api-football":
    BASE_URL = API_FOOTBALL_BASE_URL
    HEADER_KEY_NAME = API_FOOTBALL_HEADER_KEY_NAME
    HEADER_KEY_VALUE = API_FOOTBALL_HEADER_KEY_VALUE
    HEADER_HOST_NAME = API_FOOTBALL_HEADER_HOST_NAME
    HEADER_HOST_VALUE = API_FOOTBALL_HEADER_HOST_VALUE
elif CURRENT_API == "rapid-api":
    BASE_URL = RAPID_API_BASE_URL
    HEADER_KEY_NAME = RAPID_API_HEADER_KEY_NAME
    HEADER_KEY_VALUE = RAPID_API_HEADER_KEY_VALUE
    HEADER_HOST_NAME = RAPID_API_HEADER_HOST_NAME
    HEADER_HOST_VALUE = RAPID_API_HEADER_HOST_VALUE

SLEEP_TIME = 5/60


def check_subscription_status():
    url = f"{BASE_URL}/status"
    headers = {
        HEADER_KEY_NAME: HEADER_KEY_VALUE,
        HEADER_HOST_NAME: HEADER_HOST_VALUE,
    }
    response = requests.get(url, headers=headers)
    response_json = response.json()["response"]
    if response_json:
        print(
            f"Hi {response_json['account']['firstname']}!\n"
            f"Your {response_json['subscription']['plan']} plan is active until {response_json['subscription']['end']}\n"
            f"Current usage: {response_json['requests']['current']}/{response_json['requests']['limit_day']}\n"
        )
        current_pull_limit = response_json['requests']['limit_day'] - response_json['requests']['current']
        return current_pull_limit
    print("[ERROR] You have reached the request limit for today. Try again later.")
    return 0


def get_json_from_api(endpoint: str):
    url = f"{BASE_URL}/{endpoint}"
    headers = {
        HEADER_KEY_NAME: HEADER_KEY_VALUE,
        HEADER_HOST_NAME: HEADER_HOST_VALUE,
    }

    response = requests.get(url, headers=headers)
    print(f"Getting data from: {url}")
    if response.status_code == 200:
        if len(response.json()["response"]) == 0:
            print(f"Response empty. No data have been pulled.")
            return None
        if response.json()["paging"]["total"] > 1:
            print(f"[FIXME] There are more than 1 page. Please handle this asap.")
            return None
        return response
    else:
        print(f"Error fetching data from API. Status Code: {response.status_code}")
        return None


def pull_single_league_fixtures_for_all_seasons(league_id_to_pull: int, pull_limit: int) -> None:
    """
        Pull fixtures data for all seasons of a single league from an external API and write it to JSON files.

        Args:
            league_id_to_pull (int): The ID of the league to pull fixtures for.
            pull_limit (int): The maximum number of seasons to pull fixtures for.

        Returns:
            None
    """
    Session = sessionmaker(bind=get_engine())
    with Session() as session:
        try:
            league_id, league_name = session.query(League.league_id, League.name).filter(
                League.league_id == league_id_to_pull
            ).first()
            season_years = (
                session.query(Season.year).filter(Season.league_id == league_id_to_pull).all()
            )
            count = 0
            for season_year in season_years:
                if count < pull_limit:
                    print(f"Sleeping for a few seconds to avoid reaching limit.")
                    sleep(SLEEP_TIME)
                    try:
                        print(
                            f"Pulling fixtures for {league_name}, season: {season_year[0]}..."
                        )
                        season_data = get_json_from_api(
                            f"fixtures?league={league_id}&season={season_year[0]}"
                        )
                        write_response_to_json(
                            season_data, f"{league_id}-{league_name}-{season_year[0]}", "fixtures/league_seasons"
                        )
                    except Exception as e:
                        print(e)
                count += 1
        except Exception as e:
            # Handle any exceptions or errors that occur during the connection test
            print(f"Connection Error: {e}")
            raise Exception


def pull_single_league_fixtures_for_single_season(league_id_to_pull: int, season_id_to_pull: int) -> None:
    """
        Pull fixtures data for a single league and season from an external API and write it to a JSON file.

        Args:
            league_id_to_pull (int): The ID of the league to pull fixtures for.
            season_id_to_pull (int): The ID of the season to pull fixtures for.

        Returns:
            None
    """
    Session = sessionmaker(bind=get_engine())
    with Session() as session:
        try:
            league_id, league_name = session.query(League.league_id, League.name).filter(
                League.league_id == league_id_to_pull
            ).first()
            print(f"Pulling fixtures for {league_name}, season: {season_id_to_pull}...")
            season_data = get_json_from_api(
                f"fixtures?league={league_id}&season={season_id_to_pull}"
            )
            write_response_to_json(
                season_data, f"{league_id}-{league_name}-{season_id_to_pull}", "fixtures/league_seasons"
            )
        except Exception as e:
            print(e)


def pull_updated_fixtures() -> None:
    """
        Pull updated fixtures data from an external API and write it to JSON files.

        This function retrieves fixture data for specific dates and statuses from an external API
        and writes the response to JSON files. The function waits for a short duration between
        each API call to avoid exceeding rate limits.

        Returns:
            None
    """
    dates_to_pull = Fixture.get_fixtures_dates_to_be_updated()
    finished_statuses = "FT-AET-PEN-WO"

    if not dates_to_pull:
        print(f"No dates to update.")

    for single_date in dates_to_pull:
        endpoint = f"fixtures?date={single_date}&status={finished_statuses}"
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        resp = get_json_from_api(endpoint)
        write_response_to_json(resp, f"FINISHED_{single_date}_{timestamp}", "fixtures/updates")
        sleep(SLEEP_TIME)


def pull_teams_for_countries_list(country_ids_list_to_pull: list, pull_limit: int):
    Session = sessionmaker(bind=get_engine())

    with Session() as session:
        count = 0
        for country_id in country_ids_list_to_pull:
            country_name = session.query(Country.name).filter(
                Country.country_id == country_id
            ).scalar()

            if count >= pull_limit:
                break

            print(f"Sleeping for a few seconds to avoid reaching limit.")
            sleep(SLEEP_TIME)

            try:
                print(f"Pulling teams for {country_name}...")
                teams_data = get_json_from_api(f"teams?country={country_name}")
                # Check if teams_data is not empty
                if not teams_data:
                    print("No data found for", country_name)
                    continue
                write_response_to_json(teams_data, f"{country_id}_{country_name}", "teams")
                count += 1
            except Exception as e:
                print(e)
                continue


def pull_statistics_fixtures_for_leagues_and_seasons(
    league_ids_to_pull: list, season_year_to_pull: str, pull_limit: int
):
    Session = sessionmaker(bind=get_engine())
    with Session() as session:
        try:
            fixtures = (
                session.query(Fixture.fixture_id, Fixture.league_name).filter(
                    and_(
                        Fixture.league_id.in_(league_ids_to_pull),
                        Fixture.season_year == season_year_to_pull,
                        Fixture.status.in_(["FT", "AET", "PEN", "INT", "ABD"])
                    )
                ).all()
            )
            count = 0
            for fixture in fixtures:
                if count < pull_limit:
                    print(f"Sleeping for a few seconds to avoid reaching limit.")
                    sleep(SLEEP_TIME)
                    try:
                        print(
                            f"Pulling statistics fixtures for fixture: {fixture.fixture_id} "
                            f"from league: {fixture.league_name}, season: {season_year_to_pull}..."
                        )
                        statistics_fixtures_data = get_json_from_api(
                            f"fixtures/statistics?fixture={fixture.fixture_id}"
                        )

                    except Exception as e:
                        print(e)
                        continue
                write_response_to_json(
                    statistics_fixtures_data, f"{fixture.fixture_id}_statistics_fixtures", "statistics_fixtures"
                )
                count += 1
        except Exception as e:
            raise Exception


def pull_player_statistics_for_leagues_and_seasons(
    league_ids_to_pull: list, season_year_to_pull: str, pull_limit: int
):
    Session = sessionmaker(bind=get_engine())
    with Session() as session:
        try:
            fixtures = (
                session.query(Fixture.fixture_id, Fixture.league_name).filter(
                    and_(
                        Fixture.league_id.in_(league_ids_to_pull),
                        Fixture.season_year == season_year_to_pull,
                        Fixture.status.in_(["FT", "AET", "PEN", "INT", "ABD"])
                    )
                ).all()
            )
            count = 0
            for fixture in fixtures:
                if count < pull_limit:
                    print(f"Sleeping for a few seconds to avoid reaching limit.")
                    sleep(SLEEP_TIME)
                    try:
                        print(
                            f"Pulling player statistics for fixture: {fixture.fixture_id} "
                            f"from league: {fixture.league_name}, season: {season_year_to_pull}..."
                        )
                        statistics_fixtures_data = get_json_from_api(
                            f"fixtures/players?fixture={fixture.fixture_id}"
                        )
                    except Exception as e:
                        print(e)
                        continue
                write_response_to_json(
                    statistics_fixtures_data, f"{fixture.fixture_id}_player_statistics", "player_statistics"
                )
                count += 1
        except Exception as e:
            # Handle any exceptions or errors that occur during the connection test
            print(f"Connection Error: {e}")
            raise Exception


def pull_events_for_leagues_and_seasons(
    league_ids_to_pull: list, season_year_to_pull: str, pull_limit: int
):
    Session = sessionmaker(bind=get_engine())
    finished_statuses = ["FT", "AET", "PEN", "WO"]
    with Session() as session:
        try:
            fixtures = (
                session.query(Fixture.fixture_id, Fixture.league_name).filter(
                    and_(
                        Fixture.league_id.in_(league_ids_to_pull),
                        Fixture.season_year == season_year_to_pull,
                        Fixture.status.in_(finished_statuses)
                    )
                ).all()
            )
            count = 0
            for fixture in fixtures:
                if count < pull_limit:
                    print(f"Sleeping for a few seconds to avoid reaching limit.")
                    sleep(SLEEP_TIME)
                    try:
                        print(
                            f"Pulling player statistics for fixture: {fixture.fixture_id} "
                            f"from league: {fixture.league_name}, season: {season_year_to_pull}..."
                        )
                        events_fixtures_data = get_json_from_api(
                            f"fixtures/events?fixture={fixture.fixture_id}"
                        )
                        write_response_to_json(
                            events_fixtures_data, f"{fixture.fixture_id}_events", "events"
                        )
                    except Exception as e:
                        print(e)
                count += 1
        except Exception as e:
            # Handle any exceptions or errors that occur during the connection test
            print(f"Connection Error: {e}")
            raise Exception


def write_response_to_json(response, filename, subdir=""):
    if response:
        file_path = os.path.join(SOURCE_DIR, subdir, f"{filename}.json")
        try:
            with open(file_path, "w") as file:
                json.dump(response.json(), file)
            return True
        except (AttributeError, IOError) as e:
            print(f"[ERROR] Writing response to '{file_path}' has failed: {e}")
    return False
