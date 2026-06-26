-- DEMO SEED — lets you see the prediction-accuracy chart this session instead of
-- waiting a day for the real evaluation loop to accumulate data.
--
-- It fabricates "predictions" whose target_time matches readings you've ALREADY
-- collected, with a small random error around the true value. Then the daily DAG's
-- evaluate_yesterday task (re-run it after this) will join these against the real
-- readings and populate prediction_eval.
--
-- Run with:
--   docker compose exec -T postgres psql -U carpark -d carpark < demo_seed.sql
-- Then in Airflow, re-trigger ONLY the evaluate_yesterday task (or the whole
-- daily_retrain_eval DAG), and refresh the dashboard.

-- Take ~200 real readings from the last day, make a "prediction" for each that's
-- the actual value plus noise of roughly +/- 5 lots, with made_at an hour earlier.
INSERT INTO predictions (location_id, made_at, target_time, available)
SELECT
    location_id,
    event_time - INTERVAL '2 hours'                      AS made_at,
    date_trunc('hour', event_time)                       AS target_time,
    GREATEST(0, available + (floor(random() * 11) - 5))::int AS available
FROM (
    SELECT DISTINCT ON (location_id, date_trunc('hour', event_time))
        location_id, event_time, available
    FROM readings
    WHERE event_time > now() - INTERVAL '24 hours'
    ORDER BY location_id, date_trunc('hour', event_time), event_time
) sample
ON CONFLICT (location_id, target_time, made_at) DO NOTHING;

SELECT count(*) AS seeded_predictions FROM predictions;
