"""Pure functions for FRED payloads: no network, no database.

GOTCHA that shaped this file: FRED stamps every observation with realtime_start and
realtime_end set to the day you ASKED. Hashing the whole observation would make every row
look "changed" every day and defeat deduplication. So the hash covers only what identifies
the data (series, date, value). The full observation, realtime fields included, is still
stored verbatim in `payload` (it is the vintage date of the first time we saw that value).
"""

from datetime import date, datetime, timedelta, timezone

from dagster_ingestion.common.hashing import payload_hash
from dagster_ingestion.landing import Record

DEFAULT_START_DATE = "2015-01-01"


def parse_observations(response: dict, series_id: str) -> list[Record]:
    records: list[Record] = []
    for obs in response.get("observations", []):
        day = obs.get("date")
        if not day:
            continue
        records.append(
            Record(
                key=f"{series_id}:{day}",
                payload_hash=payload_hash(
                    {"series_id": series_id, "date": day, "value": obs.get("value")}
                ),
                observed_at=datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc),
                payload=obs,
            )
        )
    return records


def observation_start(
    latest_observed_at: datetime | None, lookback_days: int, full_refresh: bool = False
) -> str:
    """First date to request.

    We re-request a trailing window (not just "after the last date") so that revised
    values inside it are picked up; unchanged rows are skipped by the dedupe index.
    """
    if full_refresh or latest_observed_at is None:
        return DEFAULT_START_DATE
    start: date = latest_observed_at.date() - timedelta(days=lookback_days)
    return max(start, date.fromisoformat(DEFAULT_START_DATE)).isoformat()
