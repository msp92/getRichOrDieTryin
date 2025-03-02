SET timezone TO 'UTC';
WITH upcoming AS (
SELECT
        fixture_id,
        country_name,
        league_name,
        season_stage,
        round,
        date,
        referee,
        home_team_id AS h_id,
        home_team_name AS home,
        away_team_id AS a_id,
        away_team_name AS away
FROM dw_fixtures.fixtures
WHERE DATE BETWEEN CURRENT_TIMESTAMP AND CURRENT_DATE + INTERVAL '5 days'
AND (home_team_id IN (SELECT team_id FROM analytics_breaks.breaks_team_stats)
        OR
     away_team_id IN (SELECT team_id FROM analytics_breaks.breaks_team_stats)
     )
ORDER BY date desc
)
SELECT
        u.fixture_id,
        u.country_name,
        u.league_name,
        u.season_stage,
        u.round,
        u.date,
        u.referee,
        u.h_id,
        u.home,
        c1.coach_name AS h_coach,
        u.a_id,
        u.away,
        c2.coach_name AS a_coach
FROM upcoming u
LEFT JOIN
    dw_main.coaches c1
ON
    c1.team_id = u.h_id
    AND (
        (c1.start_date <= u.date AND (c1.end_date IS NULL OR c1.end_date >= u.date))
    )
LEFT JOIN
    dw_main.coaches c2
ON
    c2.team_id = u.a_id
    AND (
        (c2.start_date <= u.date AND (c2.end_date IS NULL OR c2.end_date >= u.date))
    )
ORDER BY date asc