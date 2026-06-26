"""Weather helpers (Open-Meteo, no API key needed).

Two endpoints:
  - forecast/current  -> live conditions (used by the producer)
  - archive           -> historical hourly weather (used by feature building and the
                         daily evaluation backfill)

Returns pandas DataFrames indexed by hour so they can be joined to carpark readings.
"""
from __future__ import annotations

import datetime as dt

import pandas as pd
import requests

from common import config
from common.logging_setup import get_logger

log = get_logger(__name__)

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARS = "temperature_2m,precipitation,wind_speed_10m"


def historical_weather(start: dt.date, end: dt.date) -> pd.DataFrame:
    """Hourly weather for [start, end] at the configured Singapore location."""
    params = {
        "latitude": config.WEATHER_LAT,
        "longitude": config.WEATHER_LON,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": HOURLY_VARS,
        "timezone": "UTC",
    }
    resp = requests.get(ARCHIVE_URL, params=params, timeout=30)
    resp.raise_for_status()
    return _to_frame(resp.json().get("hourly", {}))


def forecast_weather(hours: int = 6) -> pd.DataFrame:
    """Next-`hours` hourly forecast — used to build features for future predictions."""
    params = {
        "latitude": config.WEATHER_LAT,
        "longitude": config.WEATHER_LON,
        "hourly": HOURLY_VARS,
        "forecast_hours": hours,
        "timezone": "UTC",
    }
    resp = requests.get(FORECAST_URL, params=params, timeout=30)
    resp.raise_for_status()
    return _to_frame(resp.json().get("hourly", {}))


def _to_frame(hourly: dict) -> pd.DataFrame:
    if not hourly or "time" not in hourly:
        log.warning("Empty weather response")
        return pd.DataFrame(columns=["weather_time", "temp", "precip", "wind"])
    df = pd.DataFrame({
        "weather_time": pd.to_datetime(hourly["time"], utc=True),
        "temp": hourly.get("temperature_2m"),
        "precip": hourly.get("precipitation"),
        "wind": hourly.get("wind_speed_10m"),
    })
    return df


def join_nearest_hour(readings: pd.DataFrame, weather: pd.DataFrame) -> pd.DataFrame:
    """Attach the weather row from each reading's containing hour.

    readings needs an 'event_time' (tz-aware UTC) column.
    """
    if readings.empty or weather.empty:
        return readings.assign(temp=None, precip=None, wind=None)
    r = readings.copy()
    r["hour_floor"] = r["event_time"].dt.floor("h")
    w = weather.rename(columns={"weather_time": "hour_floor"})
    return r.merge(w, on="hour_floor", how="left").drop(columns="hour_floor")
