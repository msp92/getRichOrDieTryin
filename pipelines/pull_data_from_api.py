from services.api import (
    get_json_from_api,
    pull_teams_for_countries_list,
    pull_single_league_fixtures_for_all_seasons,
    pull_updated_fixtures,
    write_response_to_json,
    check_subscription_status
)

### Pull specific data from API ###

ENTITIES_TO_PULL = [
    # "countries",
    # "leagues",
    # "teams",
    # "fixtures",
    "fixtures_update",
]

pull_limit = check_subscription_status()  # Doesn't work in Rapid API

for entity in ENTITIES_TO_PULL:
    if entity == "countries" or entity == "leagues":
        data = get_json_from_api(entity)
        write_response_to_json(data, entity)
    elif entity == "teams":
        country_ids_to_pull_teams = []
        pull_teams_for_countries_list(country_ids_to_pull_teams, pull_limit)
    elif entity == "fixtures":
        league_ids_to_pull_fixtures = []
        for league in league_ids_to_pull_fixtures:
            pull_single_league_fixtures_for_all_seasons(league, pull_limit)
            # pull_single_league_fixtures_for_single_season(539, 2017)
    elif entity == "fixtures_update":
        pull_updated_fixtures()
    elif entity == "fixtures/statistics":
        # TODO: ...
        data = get_json_from_api(entity)
        write_response_to_json(data, entity)
