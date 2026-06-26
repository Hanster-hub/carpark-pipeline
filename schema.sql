-- Postgres schema for the serving database.
-- Run once at startup (docker-compose mounts this into Postgres init, see note below).
--
-- Three tables, matching what the Spark job writes and the dashboard reads:
--   readings      -- every cleaned reading (append-only; powers the anomaly baseline)
--   latest_state  -- one row per carpark, latest reading only (powers the live map)
--   predictions   -- model predictions, later joined to actuals for the eval loop

CREATE TABLE IF NOT EXISTS readings (
    location_id   TEXT        NOT NULL,
    name          TEXT,
    lat           DOUBLE PRECISION,
    lon           DOUBLE PRECISION,
    available     INTEGER,
    event_time    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (location_id, event_time)
);

CREATE INDEX IF NOT EXISTS idx_readings_event_time ON readings (event_time);

CREATE TABLE IF NOT EXISTS latest_state (
    location_id   TEXT PRIMARY KEY,
    name          TEXT,
    lat           DOUBLE PRECISION,
    lon           DOUBLE PRECISION,
    available     INTEGER,
    event_time    TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS predictions (
    location_id   TEXT        NOT NULL,
    made_at       TIMESTAMPTZ NOT NULL,   -- when the prediction was generated
    target_time   TIMESTAMPTZ NOT NULL,   -- the time the prediction is for (+2h)
    available     INTEGER,                -- predicted lots
    PRIMARY KEY (location_id, target_time, made_at)
);

-- The Spark job writes the current micro-batch to this staging table, then the
-- MERGE below promotes it into latest_state (last-write-wins per carpark).
CREATE TABLE IF NOT EXISTS latest_state_staging (
    location_id   TEXT,
    name          TEXT,
    lat           DOUBLE PRECISION,
    lon           DOUBLE PRECISION,
    available     INTEGER,
    event_time    TIMESTAMPTZ
);

-- Run this MERGE after each Spark micro-batch (or on a short schedule):
-- INSERT INTO latest_state SELECT * FROM latest_state_staging
--   ON CONFLICT (location_id) DO UPDATE SET
--     name = EXCLUDED.name, lat = EXCLUDED.lat, lon = EXCLUDED.lon,
--     available = EXCLUDED.available, event_time = EXCLUDED.event_time;

-- Streamed weather readings (from the weather_raw Kafka topic via Spark).
CREATE TABLE IF NOT EXISTS weather (
    event_time   TIMESTAMPTZ NOT NULL,
    temp         DOUBLE PRECISION,
    precip       DOUBLE PRECISION,
    wind         DOUBLE PRECISION,
    PRIMARY KEY (event_time)
);

-- Predicted-vs-actual comparison, written by the daily evaluation task.
-- The dashboard reads this to chart accuracy.
CREATE TABLE IF NOT EXISTS prediction_eval (
    location_id   TEXT        NOT NULL,
    target_time   TIMESTAMPTZ NOT NULL,
    predicted     DOUBLE PRECISION,
    actual        DOUBLE PRECISION,
    abs_error     DOUBLE PRECISION,
    evaluated_at  TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (location_id, target_time)
);

-- Separate database for Airflow's own metadata (used by the airflow service).
-- Safe to run once at init; ignored if it already exists on later starts.
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
