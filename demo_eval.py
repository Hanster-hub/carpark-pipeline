"""Demo evaluation — runs evaluate_yesterday over ALL predictions/actuals (not just
yesterday's calendar day), so the prediction_eval table and dashboard chart populate
immediately after running demo_seed.sql.

Run inside the airflow container (it has the deps + PYTHONPATH):
  docker compose exec airflow python /opt/airflow/demo_eval.py
"""
import pandas as pd
from sqlalchemy import create_engine

from common import config
from ml.model import evaluate_yesterday


def main():
    engine = create_engine(config.PG_URI)
    preds = pd.read_sql(
        "SELECT location_id, target_time, available FROM predictions", engine)
    actuals = pd.read_sql(
        "SELECT location_id, event_time AS target_time, available FROM readings", engine)
    if preds.empty or actuals.empty:
        print(f"Nothing to evaluate (preds={len(preds)} actuals={len(actuals)}).")
        return
    for df in (preds, actuals):
        df["target_time"] = pd.to_datetime(df["target_time"], utc=True).dt.floor("h")
    actuals = actuals.groupby(
        ["location_id", "target_time"], as_index=False)["available"].mean()
    evaluate_yesterday(preds, actuals)
    print("Done — check the prediction_eval table and the dashboard.")


if __name__ == "__main__":
    main()
