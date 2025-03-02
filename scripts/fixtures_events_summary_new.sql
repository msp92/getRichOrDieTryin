WITH wfb_prep AS (
SELECT
   f.fixture_id,
   FIRST_VALUE(CASE WHEN event_type = 'Goal' THEN team_name ELSE event_type END) OVER (PARTITION BY f.fixture_id ORDER BY event_id ASC) AS first_goal,
   CASE WHEN goals_home > goals_away THEN home_team_name WHEN goals_home < goals_away THEN away_team_name END AS winner,
   CASE WHEN ((goals_home_ht > goals_away_ht AND goals_home < goals_away) OR (goals_home_ht < goals_away_ht AND goals_home > goals_away)) THEN 'Y' END AS break,
   CASE
      WHEN (goals_home_ht > goals_away_ht AND goals_home > goals_away) THEN '1/1'
      WHEN (goals_home_ht > goals_away_ht AND goals_home < goals_away) THEN '1/2'
      WHEN (goals_home_ht > goals_away_ht AND goals_home = goals_away) THEN '1/X'
      WHEN (goals_home_ht < goals_away_ht AND goals_home < goals_away) THEN '2/2'
      WHEN (goals_home_ht < goals_away_ht AND goals_home > goals_away) THEN '2/1'
      WHEN (goals_home_ht < goals_away_ht AND goals_home = goals_away) THEN '2/X'
      WHEN (goals_home_ht = goals_away_ht AND goals_home = goals_away) THEN 'X/X'
      WHEN (goals_home_ht = goals_away_ht AND goals_home > goals_away) THEN 'X/1'
      WHEN (goals_home_ht = goals_away_ht AND goals_home < goals_away) THEN 'X/2'
   END AS "ht/ft"
FROM dw_fixtures.fixtures_events e JOIN dw_fixtures.fixtures f ON e.fixture_id = f.fixture_id
WHERE event_type = 'Goal'
GROUP BY f.fixture_id, event_id, event_type, team_name
),
wfb AS (
SELECT
   f.fixture_id,
   w.first_goal,
   w.winner,
   w.break,
   CASE WHEN ((goals_home > goals_away AND first_goal <> winner) OR goals_home < goals_away AND first_goal <> winner) THEN 'Y' END AS wfb,
   w."ht/ft"
FROM wfb_prep w JOIN dw_fixtures.fixtures_events e ON e.fixture_id = w.fixture_id JOIN dw_fixtures.fixtures f ON e.fixture_id = f.fixture_id
GROUP BY f.fixture_id,  w.first_goal, w.winner, w.break, w."ht/ft"
)
SELECT
   f.fixture_id,
   f.home_team_name,
   f.away_team_name,
   f.date,
   wfb.first_goal,
   wfb.winner,
   MAX(CASE WHEN elapsed_time <= 45 AND event_type = 'Goal' THEN elapsed_time END) AS last_goal_1_half,
   MIN(CASE WHEN elapsed_time > 45 AND event_type = 'Goal' THEN elapsed_time END) AS first_goal_2_half,
   MAX(CASE WHEN elapsed_time > 45 AND event_type = 'Goal' THEN elapsed_time END) AS last_goal_2_half,
   STRING_AGG(CASE WHEN event_detail = 'Red Card' THEN elapsed_time::text END, ', ') AS red_cards,
   STRING_AGG(CASE WHEN event_type = 'Var' THEN elapsed_time::text || ': ' || event_detail END, ', ') AS vars,
   wfb.break,
   wfb.wfb,
   wfb."ht/ft"
FROM dw_fixtures.fixtures_events e JOIN dw_fixtures.fixtures f ON e.fixture_id = f.fixture_id JOIN wfb ON e.fixture_id = wfb.fixture_id
WHERE e.elapsed_time IS NOT NULL
GROUP BY f.fixture_id, wfb.first_goal, wfb.winner, wfb.break, wfb.wfb, wfb."ht/ft"
HAVING MAX(CASE WHEN elapsed_time <= 45 AND event_type = 'Goal' THEN elapsed_time END) IS NOT NULL
ORDER BY f.date DESC
