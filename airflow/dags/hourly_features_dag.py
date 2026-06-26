"""Hourly feature DAG: read recent readings + weather, build features, store them.

Reads the last few hours from the `readings` table (populated by the Spark job),
joins historical weather, builds lag/time features, and writes them to a
`features` table for training and inference to consume.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "student",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

FEATURE_HOURS = 6  # how much recent history each run rebuilds features from


def build_hourly_features(**context):
    import datetime as dt
    import pandas as pd
    from sqlalchemy import create_engine

    from common import config
    from common.weather import historical_weather, join_nearest_hour
    from common.logging_setup import get_logger
    from ml.model import build_features

    log = get_logger("features_dag")
    engine = create_engine(config.PG_URI)

    since = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=FEATURE_HOURS)
    readings = pd.read_sql(
        "SELECT location_id, name, available, event_time "
        "FROM readings WHERE event_time > %(since)s ORDER BY event_time",
        engine, params={"since": since},
    )
    if readings.empty:
        log.warning("No recent readings; skipping feature build.")
        return

    readings["event_time"] = pd.to_datetime(readings["event_time"], utc=True)
    today = dt.date.today()
    weather = historical_weather(today - dt.timedelta(days=1), today)
    joined = join_nearest_hour(readings, weather)

    feats = build_features(joined)
    if feats.empty:
        log.warning("Feature frame empty after build_features; skipping write.")
        return

    cols = ["location_id", "event_time"] + \
           ["hour", "dow", "lag_1", "lag_2", "temp", "precip", "wind", "available"]
    feats[cols].to_sql("features", engine, if_exists="append", index=False)
    log.info("Wrote %d feature rows.", len(feats))


with DAG(
    dag_id="hourly_features",
    default_args=default_args,
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["batch", "features"],
) as dag:
    PythonOperator(
        task_id="build_hourly_features",
        python_callable=build_hourly_features,
    )
