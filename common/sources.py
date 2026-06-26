"""Data-source abstraction.

This is the key seam that keeps the project flexible. The producer doesn't know
or care whether records come from LTA carpark availability or a bike-share GBFS
feed. To switch sources later, you only add a new AvailabilitySource subclass and
change one line in producer/producer.py — nothing downstream changes, because every
source emits the same normalised record shape:

    {
        "location_id": str,     # carpark number, or bike station id
        "name": str,
        "lat": float,
        "lon": float,
        "available": int,       # lots free, or bikes available
        "capacity": int | None, # total lots / docks (may be unknown)
        "ts": str,              # ISO8601 UTC timestamp of the reading
    }
"""
from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod

import requests

from common import config
from common.logging_setup import get_logger

log = get_logger(__name__)


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


class AvailabilitySource(ABC):
    """Every concrete source returns a list of normalised records."""

    name: str = "base"

    @abstractmethod
    def fetch(self) -> list[dict]:
        ...


class LTACarparkSource(AvailabilitySource):
    """LTA DataMall — Carpark Availability.

    Docs: https://datamall.lta.gov.sg/  (requires a free AccountKey header)
    The endpoint paginates in pages of 500 via a $skip query parameter.
    """

    name = "lta_carpark"
    URL = "https://datamall2.mytransport.sg/ltaodataservice/CarParkAvailabilityv2"

    def __init__(self, account_key: str | None = None):
        self.account_key = account_key or config.LTA_ACCOUNT_KEY
        if not self.account_key:
            raise RuntimeError(
                "LTA_ACCOUNT_KEY is not set. Register at datamall.lta.gov.sg "
                "and put the key in your .env file."
            )

    def fetch(self) -> list[dict]:
        headers = {"AccountKey": self.account_key, "accept": "application/json"}
        records: list[dict] = []
        skip = 0
        ts = _now_iso()
        while True:
            resp = requests.get(
                self.URL, headers=headers, params={"$skip": skip}, timeout=15
            )
            resp.raise_for_status()
            page = resp.json().get("value", [])
            if not page:
                break
            for item in page:
                # LTA returns "Location" as "lat lon" string; parse defensively.
                lat, lon = _parse_latlon(item.get("Location", ""))
                records.append(
                    {
                        "location_id": item.get("CarParkID"),
                        "name": item.get("Development"),
                        "lat": lat,
                        "lon": lon,
                        "available": _safe_int(item.get("AvailableLots")),
                        "capacity": None,  # LTA doesn't report total lots
                        "ts": ts,
                    }
                )
            skip += 500
            if len(page) < 500:
                break
        log.info("Fetched %d carpark records", len(records))
        return records


class GBFSBikeShareSource(AvailabilitySource):
    """Bike-share fallback. Fill in a real station_status.json URL if you find a
    Singapore GBFS feed. Left here to show the one-file swap is real."""

    name = "gbfs_bikeshare"

    def __init__(self, status_url: str, info_url: str | None = None):
        self.status_url = status_url
        self.info_url = info_url

    def fetch(self) -> list[dict]:
        # TODO: join station_status (live bikes) with station_information (lat/lon/capacity)
        raise NotImplementedError("Wire up a GBFS feed URL if switching to bike-share.")


def _parse_latlon(s: str) -> tuple[float | None, float | None]:
    try:
        a, b = s.split()
        return float(a), float(b)
    except (ValueError, AttributeError):
        return None, None


def _safe_int(v) -> int | None:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None
