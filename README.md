# Singapore Carpark Availability — Data Engineering Pipeline

A real-time + batch data engineering pipeline that ingests live carpark
availability for ~2,600 carparks across Singapore, predicts availability two
hours ahead, evaluates the model daily against what actually happened, and serves
everything through a live dashboard with a map, forecasts, and anomaly alerts.

It is a **data engineering** project first: the machine-learning model is one
component that the pipeline feeds and serves, not the centrepiece.

---

## What it does

- **Ingests** live carpark availability from LTA DataMall and current weather from
  Open-Meteo, every 60 seconds.
- **Streams** that data through Kafka into Spark Structured Streaming, which cleans,
  validates and deduplicates it.
- **Stores** it two ways: a partitioned **Parquet** data lake (analytical history)
  and **Postgres** (fast serving state for the dashboard).
- **Builds features and trains** a model on a schedule via Airflow, tracking and
  registering it in MLflow.
- **Predicts** availability for the next two hours per carpark, and **evaluates**
  yesterday's predictions against the actuals each morning at 08:00.
- **Serves** a Streamlit dashboard: live map, current weather, per-carpark forecast,
  prediction-accuracy chart, and anomaly alerts.

---

## Architecture

```
                        REAL-TIME (streaming)
 LTA + Open-Meteo API
        -> Producer (poll 60s, retries, dedup)
        -> Kafka (topics: carpark_raw, weather_raw)
        -> Spark Structured Streaming (clean, validate, dedup)
             -> Parquet lake   (/data/lake, partitioned by date)   [analytical]
             -> Postgres        (latest_state, readings, weather)    [serving]

                        SCHEDULED (batch / MLOps, via Airflow)
   hourly_features      -> build lag/time + weather features  -> features table
   daily_retrain_eval   -> evaluate yesterday vs actuals
                        -> train + register model (MLflow)
                        -> generate next-2h predictions        -> predictions table

                        SERVING
   FastAPI   -> loads the registered model, serves /predict
   Streamlit -> live map, weather, forecast, accuracy, alerts
```

All services run together under one `docker-compose.yml`.

---

## Tech stack

| Layer | Tool | Why |
|-------|------|-----|
| Ingestion | Python producer + **Kafka** | Decouples the API poller from processing; buffers across restarts |
| Stream processing | **Spark** Structured Streaming | Distributed, checkpointed cleaning/validation/dedup |
| Analytical store | **Parquet** lake | Columnar, compressed, partitioned — ideal for training scans |
| Serving store | **Postgres** | Fast single-row lookups for the dashboard |
| Orchestration | **Airflow** | Scheduled batch: hourly features + daily 08:00 retrain/evaluate |
| ML lifecycle | **MLflow** | Experiment tracking + model registry |
| Inference | **FastAPI** | Loads the registered model, serves predictions |
| Dashboard | **Streamlit** + PyDeck | Live map, forecast, accuracy, alerts |
| Packaging | **Docker Compose** | One-command reproducible stack |

---

## Project layout

```
carpark-pipeline/
├── docker-compose.yml      # all services
├── schema.sql              # Postgres tables (auto-run on first start)
├── .env.example            # copy to .env and add your LTA key
├── common/
│   ├── config.py           # central config from env vars
│   ├── logging_setup.py     # shared structured logging
│   ├── sources.py          # pluggable data source (LTA; GBFS bike-share stub)
│   └── weather.py          # Open-Meteo forecast + historical helpers
├── producer/producer.py    # polls APIs, publishes to Kafka (retries, dedup)
├── streaming/stream_job.py # Spark: Kafka -> clean -> Parquet + Postgres (2 streams)
├── airflow/dags/
│   ├── hourly_features_dag.py    # @hourly feature build
│   └── daily_retrain_dag.py      # 08:00 evaluate -> train -> predict
├── ml/model.py             # feature build, train/register, evaluate
├── api/main.py             # FastAPI inference service
├── dashboard/app.py        # Streamlit dashboard
├── demo_seed.sql           # seed synthetic predictions for the accuracy demo
└── demo_eval.py            # run an evaluation immediately (demo)
```

---

## Prerequisites

- Docker Desktop (with Compose).
- A free **LTA DataMall AccountKey** — register at https://datamall.lta.gov.sg/.
  (Open-Meteo weather needs no key.)

---

## Quick start

1. Copy the environment template and add your key:
   ```
   cp .env.example .env            # Windows PowerShell: Copy-Item .env.example .env
   ```
   Open `.env` and paste your key into `LTA_ACCOUNT_KEY=`.

2. Build and start everything:
   ```
   docker compose up -d --build
   ```
   First run takes a few minutes (Spark downloads connector JARs; Airflow initialises).

3. Open the UIs (confirm ports with `docker compose ps`):
   - Dashboard — http://localhost:8501
   - Airflow   — http://localhost:8080  (login `admin` / `admin`)
   - MLflow    — http://localhost:5000
   - API docs  — http://localhost:8000/docs

4. Verify data is flowing:
   ```
   docker compose exec postgres psql -U carpark -d carpark -c "SELECT count(*) FROM latest_state;"
   ```
   A non-zero count means the real-time path is working.

---

## Producing predictions

The model is created by Airflow, so trigger the DAGs once to bootstrap it:

1. In Airflow, enable and run **`hourly_features`** a few times to build up the
   `features` table.
2. Then run **`daily_retrain_eval`**. Its tasks run in order:
   `evaluate_yesterday` → `train_and_register` → `generate_predictions`.
3. Confirm a model registered in MLflow (Models tab), then reload the inference API:
   ```
   docker compose restart api
   ```
4. Refresh the dashboard — the per-carpark forecast now works.

> The daily `evaluate_yesterday` step only has data to compare once predictions have
> been sitting for a day. To see the accuracy chart immediately, use the demo below.

### Demo: see the accuracy chart now

```
# seed synthetic predictions matching readings you've already collected
Get-Content demo_seed.sql | docker compose exec -T postgres psql -U carpark -d carpark   # PowerShell
# (bash:  docker compose exec -T postgres psql -U carpark -d carpark < demo_seed.sql)

# run an evaluation over all predictions vs actuals
docker compose exec airflow python /opt/airflow/demo_eval.py
```
Refresh the dashboard — the predicted-vs-actual scatter and MAE appear.
**Note:** these numbers are synthetic; they demonstrate the loop, not real accuracy.

---

## Dashboard features

- **Live map** — every carpark plotted; colour scales red→green by availability
  relative to peers; hover a point for its name and current free lots.
- **Current weather** — streamed temperature, rain, and wind.
- **2-hour forecast** — pick any carpark; predicts for the real target time using
  current weather.
- **Prediction accuracy** — predicted-vs-actual scatter and mean absolute error.
- **Anomaly alerts** — a rolling z-score flags carparks unusually full or empty.

---

## Robustness features

- Producer **retries** with exponential backoff on API failures.
- **Deduplication** at the source and in-stream (Spark watermark).
- **Data-quality checks** drop null IDs, impossible coordinates, negative values.
- **Idempotent upserts** (staging + MERGE / `ON CONFLICT`) — safe to re-run.
- **Airflow retries** so transient task failures self-recover.
- **Model monitoring** — daily MAE logged to MLflow as a drift signal.
- **Structured logging** across every service.

---

## Daily operations

Stop everything (keeps your data):
```
docker compose down
```

Start it again later (fast — images and JARs are cached):
```
docker compose up -d
```
If the streaming job lost the startup race with Kafka: `docker compose restart spark`.
If the dashboard says predictions aren't reachable: `docker compose restart api`.

> Do **not** use `docker compose down -v` unless you intend to wipe all stored data
> (readings, weather, the trained model). Plain `down` preserves the volumes.

---

## Switching the data source (e.g. to bike-share)

Everything downstream consumes a normalised record shape, so the source is pluggable.
To switch, add a source class in `common/sources.py` (a `GBFSBikeShareSource` stub is
included) and change one line in `producer/producer.py`. Nothing else changes.

---

## Notes & limitations

- LTA does not report each carpark's total capacity, so the map colours availability
  *relative to other carparks*, not as a true "percent full".
- The model is intentionally simple; the engineering value is in the surrounding
  pipeline and MLOps loop, not model sophistication.
- Weather accumulates more slowly than carpark data — Open-Meteo's current reading
  changes roughly every 15 minutes and unchanged values are deduplicated.
