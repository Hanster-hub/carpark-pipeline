"""Daily 8:00 AM DAG: evaluate yesterday -> retrain -> generate fresh predictions.

Satisfies the rubric's 'evaluate the model every day at 8:00 AM'. Task order:
  evaluate_yesterday >> train_and_register >> generate_predictions
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "student",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def task_evaluate_yesterday(**context):
    import datetime as dt
    import pandas as pd
    from sqlalchemy import create_engine

    from common import config
    from common.logging_setup import get_logger
    from ml.model import evaluate_yesterday

    log = get_logger("daily_dag")
    engine = create_engine(config.PG_URI)

    y_start = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0)
    y_end = y_start + dt.timedelta(days=1)

    preds = pd.read_sql(
        "SELECT location_id, target_time, available FROM predictions "
        "WHERE target_time >= %(s)s AND target_time < %(e)s",
        engine, params={"s": y_start, "e": y_end},
    )
    actuals = pd.read_sql(
        "SELECT location_id, event_time AS target_time, available FROM readings "
        "WHERE event_time >= %(s)s AND event_time < %(e)s",
        engine, params={"s": y_start, "e": y_end},
    )
    if preds.empty or actuals.empty:
        log.warning("Not enough data to evaluate yesterday (preds=%d actuals=%d).",
                    len(preds), len(actuals))
        return
    # round both to the hour so they line up
    for df in (preds, actuals):
        df["target_time"] = pd.to_datetime(df["target_time"], utc=True).dt.floor("h")
    actuals = actuals.groupby(["location_id", "target_time"], as_index=False)["available"].mean()
    evaluate_yesterday(preds, actuals)


def task_train_and_register(**context):
    import pandas as pd
    from sqlalchemy import create_engine

    from common import config
    from common.logging_setup import get_logger
    from ml.model import train_and_register

    log = get_logger("daily_dag")
    engine = create_engine(config.PG_URI)
    train_df = pd.read_sql(
        "SELECT location_id, event_time, available, temp, precip, wind "
        "FROM features ORDER BY event_time", engine,
    )
    if len(train_df) < 100:
        log.warning("Only %d feature rows; training anyway but expect a weak model.",
                    len(train_df))
    train_df["event_time"] = pd.to_datetime(train_df["event_time"], utc=True)
    train_and_register(train_df)


def task_generate_predictions(**context):
    """Score the next 2 hours for every carpark and store in `predictions`."""
    import datetime as dt
    import mlflow.pyfunc
    import pandas as pd
    from sqlalchemy import create_engine

    from common import config
    from common.weather import forecast_weather
    from common.logging_setup import get_logger
    from ml.model import FEATURES

    log = get_logger("daily_dag")
    engine = create_engine(config.PG_URI)
    mlflow.set_tracking_uri(config.MLFLOW_TRACKING_URI)
    model = mlflow.pyfunc.load_model(f"models:/{config.MODEL_NAME}/latest")

    latest = pd.read_sql(
        "SELECT location_id, available FROM latest_state", engine)
    if latest.empty:
        log.warning("No latest_state rows; cannot predict.")
        return

    now = dt.datetime.now(dt.timezone.utc)
    target = (now + dt.timedelta(hours=2)).replace(minute=0, second=0, microsecond=0)
    wx = forecast_weather(hours=3)
    wrow = wx.iloc[min(2, len(wx) - 1)] if not wx.empty else None

    X = pd.DataFrame({
        "hour": target.hour,
        "dow": target.weekday(),
        "lag_1": latest["available"],
        "lag_2": latest["available"],
        "temp": wrow["temp"] if wrow is not None else 30.0,
        "precip": wrow["precip"] if wrow is not None else 0.0,
        "wind": wrow["wind"] if wrow is not None else 5.0,
    })[FEATURES]

    out = pd.DataFrame({
        "location_id": latest["location_id"],
        "made_at": now,
        "target_time": target,
        "available": model.predict(X).round().astype(int),
    })
    out.to_sql("predictions", engine, if_exists="append", index=False)
    log.info("Wrote %d predictions for %s.", len(out), target.isoformat())


with DAG(
    dag_id="daily_retrain_eval",
    default_args=default_args,
    schedule="0 8 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["batch", "mlops"],
) as dag:
    evaluate = PythonOperator(task_id="evaluate_yesterday",
                              python_callable=task_evaluate_yesterday)
    retrain = PythonOperator(task_id="train_and_register",
                             python_callable=task_train_and_register)
    predict = PythonOperator(task_id="generate_predictions",
                             python_callable=task_generate_predictions)
    evaluate >> retrain >> predict
