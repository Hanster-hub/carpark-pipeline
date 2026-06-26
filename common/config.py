"""Central config. Everything reads from environment variables (see .env.example).

Keeping config in one place means the producer, Spark job, Airflow DAGs, API and
dashboard all agree on topic names, paths and connection strings.
"""
import os


# --- Kafka ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_CARPARK_RAW = os.getenv("TOPIC_CARPARK_RAW", "carpark_raw")
TOPIC_WEATHER_RAW = os.getenv("TOPIC_WEATHER_RAW", "weather_raw")

# --- Data lake (Parquet) ---
LAKE_ROOT = os.getenv("LAKE_ROOT", "/data/lake")
CARPARK_LAKE = f"{LAKE_ROOT}/carpark"      # partitioned by date=YYYY-MM-DD
WEATHER_LAKE = f"{LAKE_ROOT}/weather"

# --- Serving DB (Postgres) ---
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "carpark")
PG_USER = os.getenv("PG_USER", "carpark")
PG_PASSWORD = os.getenv("PG_PASSWORD", "carpark")  # dev only; never commit real secrets
PG_URI = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# --- MLflow ---
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_EXPERIMENT = os.getenv("MLFLOW_EXPERIMENT", "carpark_availability")
MODEL_NAME = os.getenv("MODEL_NAME", "carpark_predictor")

# --- Data source credentials ---
# Register free at https://datamall.lta.gov.sg/ to get an AccountKey.
# Put it in your .env file — do NOT hardcode it here or commit it.
LTA_ACCOUNT_KEY = os.getenv("LTA_ACCOUNT_KEY", "")

# Singapore bounding-box centroid, used for the weather pull (Open-Meteo, no key needed).
WEATHER_LAT = float(os.getenv("WEATHER_LAT", "1.3521"))
WEATHER_LON = float(os.getenv("WEATHER_LON", "103.8198"))

# --- Polling ---
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
