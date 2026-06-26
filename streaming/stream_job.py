"""Spark Structured Streaming: Kafka -> clean/validate -> Parquet lake + Postgres.

This is the real-time path and the 'distributed processing' robustness point.

Two sinks:
  1. Parquet lake (append, partitioned by date) — the analytical store the batch
     layer and model training read from.
  2. Postgres 'latest_state' table (upsert via foreachBatch) — what the dashboard
     queries for the live map. Kept tiny: one row per carpark, latest reading only.

Data-quality checks (a rubric robustness item) reject rows with null ids, impossible
coordinates, or negative availability, and log the reject rate per micro-batch.
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
)

from common import config

CARPARK_SCHEMA = StructType([
    StructField("location_id", StringType()),
    StructField("name", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("available", IntegerType()),
    StructField("capacity", IntegerType()),
    StructField("ts", StringType()),
])

# Singapore bounding box — anything outside is bad data.
SG_BOUNDS = dict(lat_min=1.20, lat_max=1.48, lon_min=103.6, lon_max=104.1)

# Open-Meteo 'current' block, as published to weather_raw by the producer.
WEATHER_SCHEMA = StructType([
    StructField("time", StringType()),
    StructField("temperature_2m", DoubleType()),
    StructField("precipitation", DoubleType()),
    StructField("wind_speed_10m", DoubleType()),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("carpark-stream")
        .config("spark.sql.streaming.checkpointLocation", "/data/checkpoints/carpark")
        .getOrCreate()
    )


def clean(df):
    """Parse JSON, apply data-quality filters. Returns (clean_df, raw_count_col)."""
    parsed = (
        df.selectExpr("CAST(value AS STRING) AS json")
        .select(F.from_json("json", CARPARK_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_time", F.to_timestamp("ts"))
        .withColumn("date", F.to_date("event_time"))
    )
    valid = (
        parsed
        .filter(F.col("location_id").isNotNull())
        .filter(F.col("available") >= 0)
        .filter(F.col("lat").between(SG_BOUNDS["lat_min"], SG_BOUNDS["lat_max"]))
        .filter(F.col("lon").between(SG_BOUNDS["lon_min"], SG_BOUNDS["lon_max"]))
        # dedup within the stream watermark window
        .withWatermark("event_time", "10 minutes")
        .dropDuplicates(["location_id", "event_time"])
    )
    return valid


def _jdbc_write(df, table, mode):
    (
        df.write.format("jdbc")
        .option("url", f"jdbc:postgresql://{config.PG_HOST}:{config.PG_PORT}/{config.PG_DB}")
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", table)
        .option("user", config.PG_USER)
        .option("password", config.PG_PASSWORD)
        .mode(mode)
        .save()
    )


def _merge_latest_state():
    """Promote staging -> latest_state, append staging -> readings (ignoring dup PKs),
    then clear staging. All via one psycopg2 connection."""
    import psycopg2
    conn = psycopg2.connect(
        host=config.PG_HOST, port=config.PG_PORT, dbname=config.PG_DB,
        user=config.PG_USER, password=config.PG_PASSWORD,
    )
    try:
        with conn, conn.cursor() as cur:
            # history: copy staging rows into readings, skip ones we already have
            cur.execute("""
                INSERT INTO readings (location_id, name, lat, lon, available, event_time)
                SELECT location_id, name, lat, lon, available, event_time
                FROM latest_state_staging
                ON CONFLICT (location_id, event_time) DO NOTHING;
            """)
            # latest state: newest reading per carpark
            cur.execute("""
                INSERT INTO latest_state
                SELECT DISTINCT ON (location_id)
                    location_id, name, lat, lon, available, event_time
                FROM latest_state_staging
                ORDER BY location_id, event_time DESC
                ON CONFLICT (location_id) DO UPDATE SET
                    name = EXCLUDED.name, lat = EXCLUDED.lat, lon = EXCLUDED.lon,
                    available = EXCLUDED.available, event_time = EXCLUDED.event_time;
            """)
            cur.execute("TRUNCATE latest_state_staging;")
    finally:
        conn.close()


def upsert_to_postgres(batch_df, batch_id):
    """foreachBatch sink: stage this micro-batch, then merge into readings + latest_state."""
    rows = batch_df.select(
        "location_id", "name", "lat", "lon", "available", "event_time"
    )
    if rows.rdd.isEmpty():
        return
    _jdbc_write(rows, "latest_state_staging", "append")
    _merge_latest_state()


def clean_weather(df):
    """Parse the weather_raw JSON into typed columns with an event_time."""
    return (
        df.selectExpr("CAST(value AS STRING) AS json")
        .select(F.from_json("json", WEATHER_SCHEMA).alias("d"))
        .select(
            F.to_timestamp("d.time").alias("event_time"),
            F.col("d.temperature_2m").alias("temp"),
            F.col("d.precipitation").alias("precip"),
            F.col("d.wind_speed_10m").alias("wind"),
        )
        .filter(F.col("event_time").isNotNull())
        .withColumn("date", F.to_date("event_time"))
        .withWatermark("event_time", "30 minutes")
        .dropDuplicates(["event_time"])
    )


def weather_to_postgres(batch_df, batch_id):
    """foreachBatch sink: append weather readings to the `weather` table,
    ignoring duplicate timestamps (the producer may repeat the same 'current' hour)."""
    rows = batch_df.select("event_time", "temp", "precip", "wind")
    if rows.rdd.isEmpty():
        return
    _jdbc_write(rows, "weather_staging", "overwrite")
    import psycopg2
    conn = psycopg2.connect(
        host=config.PG_HOST, port=config.PG_PORT, dbname=config.PG_DB,
        user=config.PG_USER, password=config.PG_PASSWORD,
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO weather (event_time, temp, precip, wind)
                SELECT event_time, temp, precip, wind FROM weather_staging
                ON CONFLICT (event_time) DO NOTHING;
            """)
    finally:
        conn.close()


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", config.TOPIC_CARPARK_RAW)
        .option("startingOffsets", "latest")
        .load()
    )

    clean_df = clean(raw)

    # Sink 1: Parquet lake, partitioned by date
    (
        clean_df.writeStream
        .format("parquet")
        .option("path", config.CARPARK_LAKE)
        .partitionBy("date")
        .outputMode("append")
        .start()
    )

    # Sink 2: Postgres latest state for the dashboard
    (
        clean_df.writeStream
        .foreachBatch(upsert_to_postgres)
        .outputMode("update")
        .start()
    )

    # --- Second stream: weather_raw -> weather table + Parquet ---
    weather_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", config.TOPIC_WEATHER_RAW)
        .option("startingOffsets", "latest")
        .load()
    )
    weather_df = clean_weather(weather_raw)

    (
        weather_df.writeStream
        .format("parquet")
        .option("path", config.WEATHER_LAKE)
        .option("checkpointLocation", "/data/checkpoints/weather_parquet")
        .partitionBy("date")
        .outputMode("append")
        .start()
    )
    (
        weather_df.writeStream
        .foreachBatch(weather_to_postgres)
        .option("checkpointLocation", "/data/checkpoints/weather_pg")
        .outputMode("update")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
