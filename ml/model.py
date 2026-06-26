"""Model training, evaluation and the predict-vs-actual comparison, all logged to MLflow.

Keep the model boring on purpose — a gradient-boosting regressor on simple features
(hour, day-of-week, recent availability lag, weather) is plenty. The marks are for the
MLOps loop around it (track -> evaluate -> register -> serve -> monitor), not model
sophistication.

Called by the Airflow DAGs:
  - train_and_register()  -> daily retrain, logs metrics, registers if better
  - evaluate_yesterday()  -> compares yesterday's predictions against actuals,
                              logs accuracy as a monitoring signal
"""
import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error

from common import config
from common.logging_setup import get_logger

log = get_logger(__name__)

FEATURES = ["hour", "dow", "lag_1", "lag_2", "temp", "precip", "wind"]
TARGET = "available"


def _setup_mlflow():
    mlflow.set_tracking_uri(config.MLFLOW_TRACKING_URI)
    mlflow.set_experiment(config.MLFLOW_EXPERIMENT)


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Turn raw readings (joined with weather) into model features.

    Expects columns: location_id, event_time, available, temp, precip, wind.
    This is also what the hourly Airflow feature DAG materialises to Parquet.
    """
    df = df.sort_values(["location_id", "event_time"]).copy()
    df["hour"] = df["event_time"].dt.hour
    df["dow"] = df["event_time"].dt.dayofweek
    df["lag_1"] = df.groupby("location_id")["available"].shift(1)
    df["lag_2"] = df.groupby("location_id")["available"].shift(2)
    return df.dropna(subset=FEATURES + [TARGET])


def train_and_register(train_df: pd.DataFrame):
    """Fit on recent data, log to MLflow, register the model version."""
    _setup_mlflow()
    feat = build_features(train_df)
    X, y = feat[FEATURES], feat[TARGET]

    with mlflow.start_run(run_name="daily_retrain") as run:
        model = GradientBoostingRegressor(n_estimators=200, max_depth=4)
        model.fit(X, y)
        mae = mean_absolute_error(y, model.predict(X))  # use a holdout in practice

        mlflow.log_params({"n_estimators": 200, "max_depth": 4, "n_rows": len(X)})
        mlflow.log_metric("train_mae", mae)
        mlflow.sklearn.log_model(
            model, artifact_path="model", registered_model_name=config.MODEL_NAME
        )
        log.info("Trained & registered %s, train_mae=%.3f (run %s)",
                 config.MODEL_NAME, mae, run.info.run_id)


def evaluate_yesterday(predictions_df: pd.DataFrame, actuals_df: pd.DataFrame):
    """Compare yesterday's stored predictions with the actuals that have now arrived.

    Logs MAE to MLflow (drift signal) AND writes the per-carpark comparison to the
    `prediction_eval` table so the dashboard can chart predicted vs actual.
    """
    _setup_mlflow()
    merged = predictions_df.merge(
        actuals_df, on=["location_id", "target_time"], suffixes=("_pred", "_act")
    )
    if merged.empty:
        log.warning("No overlapping predictions/actuals to evaluate.")
        return
    mae = mean_absolute_error(merged["available_act"], merged["available_pred"])
    with mlflow.start_run(run_name="daily_evaluation"):
        mlflow.log_metric("yesterday_mae", mae)
        mlflow.log_metric("n_compared", len(merged))
    log.info("Yesterday evaluation: MAE=%.3f over %d points", mae, len(merged))

    # write the comparison to Postgres for the dashboard
    from sqlalchemy import create_engine
    out = pd.DataFrame({
        "location_id": merged["location_id"],
        "target_time": merged["target_time"],
        "predicted": merged["available_pred"].astype(float),
        "actual": merged["available_act"].astype(float),
        "abs_error": (merged["available_act"] - merged["available_pred"]).abs().astype(float),
    })
    engine = create_engine(config.PG_URI)
    # upsert: clear any existing rows for these target times, then insert
    out.to_sql("prediction_eval_staging", engine, if_exists="replace", index=False)
    import psycopg2
    conn = psycopg2.connect(
        host=config.PG_HOST, port=config.PG_PORT, dbname=config.PG_DB,
        user=config.PG_USER, password=config.PG_PASSWORD,
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO prediction_eval
                    (location_id, target_time, predicted, actual, abs_error)
                SELECT location_id, target_time, predicted, actual, abs_error
                FROM prediction_eval_staging
                ON CONFLICT (location_id, target_time) DO UPDATE SET
                    predicted = EXCLUDED.predicted, actual = EXCLUDED.actual,
                    abs_error = EXCLUDED.abs_error, evaluated_at = now();
                DROP TABLE IF EXISTS prediction_eval_staging;
            """)
    finally:
        conn.close()
    log.info("Wrote %d comparison rows to prediction_eval.", len(out))
