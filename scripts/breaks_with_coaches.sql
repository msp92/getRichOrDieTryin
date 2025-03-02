SELECT
    b.fixture_id,
    b.country_name,
    b.league_id,
    b.league_name,
    b.season_year,
    b.season_stage,
    b.round,
    b.date,
    b.referee,
    b.home_team_id,
    b.home_team_name,
    c1.coach_name AS home_team_coach,
    b.away_team_id,
    b.away_team_name,
    c2.coach_name AS away_team_coach,
    b.goals_home,
    b.goals_away,
    b.goals_home_ht,
    b.goals_away_ht
FROM
    analytics_breaks.breaks b
LEFT JOIN
    dw_main.coaches c1
ON
    c1.team_id = b.home_team_id
    AND (
        (c1.start_date <= b.date AND (c1.end_date IS NULL OR c1.end_date >= b.date))
    )
LEFT JOIN
    dw_main.coaches c2
ON
    c2.team_id = b.away_team_id
    AND (
        (c2.start_date <= b.date AND (c2.end_date IS NULL OR c2.end_date >= b.date))
    )
WHERE b.date > '2019-12-31' ORDER BY b.date DESC