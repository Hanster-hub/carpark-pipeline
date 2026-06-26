# Carpark Availability — Data Engineering Pipeline

Real-time + batch pipeline that ingests Singapore carpark availability (LTA DataMall)
and weather (Open-Meteo), predicts availability for the next 2 hours, evaluates the
model daily, and serves a live dashboard with anomaly alerts.

## Architecture

```
LTA API + Open-Meteo
   -> Producer (poll 60s, retries, dedup)
   -> Kafka (carpark_raw, weather_raw)
   -> Spark Structured Streaming (clean, validate, dedup)
        -> Parquet lake (partitioned by date)      [analytical store]
        -> Postgres latest_state                   [serving store]
Airflow:
   -> hourly_features        (build feature table from the lake)
   -> daily_retrain_eval     (08:00: evaluate yesterday, then retrain)
MLflow: track -> evaluate -> register
FastAPI: loads registered model, serves /predict
Streamlit dashboard: live map, predictions, predicted-vs-actual, alerts
```

## Quick start

1. Register at https://datamall.lta.gov.sg/ for a free AccountKey.
2. `cp .env.example .env` and paste your key into `LTA_ACCOUNT_KEY`.
3. `docker compose up --build`
4. Open: dashboard http://localhost:8501 · Airflow http://localhost:8080 · MLflow http://localhost:5000

## What runs out of the box vs what you build

This is a skeleton, not a finished system. After `docker compose up`:

Works immediately: all services start, Postgres auto-creates its tables from
`schema.sql`, the producer pulls real LTA + weather data and publishes to Kafka,
the Spark job consumes and writes Parquet + the staging table.

You fill these in (marked `TODO` in the code), following the build order below:
- Spark: the MERGE from `latest_state_staging` into `latest_state` (SQL is in `schema.sql`).
- Airflow `hourly_features`: read Parquet, join weather, write the feature table.
- Airflow `daily_retrain_eval`: load features -> `ml.model.train_and_register`;
  load yesterday's predictions + actuals -> `ml.model.evaluate_yesterday`.
- Historical weather backfill (Open-Meteo archive endpoint) for the eval loop.
- Writing predictions into the `predictions` table so the comparison chart has data.

The dashboard's live map works once `latest_state` is populated (i.e. after you
wire the MERGE). Until then it'll show an empty map — that's expected, not a bug.

## Suggested build order (de-risk first, polish last)

- Week 1 — one record end to end: producer -> Kafka -> Spark -> one Parquet file -> dashboard reads it. (Secures the 30 pipeline marks.)
- Week 2 — batch + ML loop: hourly feature DAG, MLflow training, the daily 08:00 evaluate+retrain. (Secures the 30 ML/real-time marks.)
- Week 3 — robustness + polish: retries, data-quality checks, dedup, logging, anomaly alert, tidy compose. Then slides + rehearse. (The 10 robustness marks + 30 presentation marks.)

## How each file maps to the rubric

| Rubric criterion | Where it lives |
|---|---|
| End-to-end pipeline | producer -> kafka -> streaming/stream_job.py -> Parquet + Postgres -> dashboard |
| Batch processing | airflow/dags/hourly_features_dag.py |
| Model train/inference | ml/model.py, api/main.py |
| Real-time | producer + Kafka + Spark streaming + dashboard refresh |
| Daily 8am evaluation | airflow/dags/daily_retrain_dag.py (schedule `0 8 * * *`) |
| Predicted vs actual | ml.model.evaluate_yesterday + dashboard accuracy section |
| Anomaly alerts | dashboard/app.py detect_anomalies() |
| Robustness | producer retries/dedup, Spark data-quality filters, logging, Airflow retries |
| Presentation highlights | docker-compose.yml (services), the two DAGs (retries/deps), Kafka flow |

## Switching to bike-share later

Everything downstream reads a normalised record shape. To switch sources, add a
`GBFSBikeShareSource` URL in `common/sources.py` and change one line in
`producer/producer.py`. Nothing else changes.

## Reflection (prepare for the video)

1. Most challenging part?
2. What are you most proud of?
3. What would you add with more time?
