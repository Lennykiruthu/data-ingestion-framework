"""FRED landing asset: fetch -> hash -> insert into raw.records -> emit metadata."""

from dagster import (
    AssetExecutionContext, AssetSelection, Backoff, Config, DefaultScheduleStatus, Jitter,
    MaterializeResult, MetadataValue, RetryPolicy, ScheduleDefinition, asset, define_asset_job,
)

from dagster_ingestion.common.errors import IngestionFailure
from dagster_ingestion.landing import land_records, latest_observed_at
from dagster_ingestion.ops_metrics import track_run
from dagster_ingestion.resources import FredResource, PostgresResource
from dagster_ingestion.sources.fred.parse import observation_start, parse_observations

SOURCE = "fred"

# series id -> friendly name (the friendly name is only for humans; the id is the stream)
SERIES = {
    "DFF":      "fed_funds_rate",
    "GS10":     "treasury_10y_yield",
    "CPIAUCSL": "cpi",
    "UNRATE":   "unemployment_rate",
    "USREC":    "recession_indicator",
    "T10YIE":   "breakeven_inflation",
}


class FredConfig(Config):
    full_refresh: bool = False  # re-request all history (catches old revisions, e.g. seasonal adjustment)
    lookback_days: int = 90     # trailing window re-requested every run to catch recent revisions


@asset(
    key=["raw", "fred"],
    group_name="raw",
    compute_kind="python",
    description=(
        "Macro indicators from FRED (fed funds rate, 10y yield, CPI, unemployment, recession "
        "flag, breakeven inflation). Appends one row per observation into raw.records; "
        "re-runs and overlapping windows insert nothing new."
    ),
    retry_policy=RetryPolicy(
        max_retries=3, delay=30, backoff=Backoff.EXPONENTIAL, jitter=Jitter.PLUS_MINUS
    ),
)
def raw_fred(
    context: AssetExecutionContext, config: FredConfig, pg: PostgresResource, fred: FredResource
) -> MaterializeResult:
    failures: list[IngestionFailure] = []

    with track_run(pg, context, source=SOURCE) as tally:
        for series_id in SERIES:
            try:
                with pg.connection() as conn:
                    latest = latest_observed_at(conn, SOURCE, series_id)
                start = observation_start(latest, config.lookback_days, config.full_refresh)

                fetched = fred.fetch_observations(series_id, start)
                records = parse_observations(fetched.data, series_id)

                with pg.connection() as conn:
                    inserted = land_records(
                        conn, source=SOURCE, stream=series_id, run_id=context.run.run_id,
                        request_url=fetched.request_url, records=records,
                    )
                tally.add(
                    series_id, fetched=len(records), inserted=inserted,
                    bytes_fetched=fetched.bytes_fetched, latency_ms=fetched.latency_ms,
                    http_status=fetched.http_status,
                )
                context.log.info(
                    f"{series_id}: from {start}, fetched {len(records)}, new {inserted}"
                )
            except IngestionFailure as exc:
                # One bad series must not stop the others from landing.
                context.log.error(f"{series_id}: {exc.description}")
                failures.append(exc)

        if failures:
            raise IngestionFailure(
                f"{len(failures)} of {len(SERIES)} series failed: "
                + "; ".join(f.description or "" for f in failures),
                category=failures[0].category,
                retryable=all(f.allow_retries for f in failures),
            )

    return MaterializeResult(
        metadata={
            "rows_fetched": tally.rows_fetched,
            "rows_inserted": tally.rows_inserted,
            "bytes_fetched": tally.bytes_fetched,
            "streams": MetadataValue.json(tally.streams),
        }
    )


fred_job = define_asset_job(
    "fred_daily",
    selection=AssetSelection.assets(raw_fred),
    tags={"source": SOURCE},  # read by the run-failure sensor
)

# 06:00 UTC = 09:00 Nairobi (no DST), early morning in the US, after FRED's overnight updates.
fred_schedule = ScheduleDefinition(
    job=fred_job,
    cron_schedule="0 6 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
)
