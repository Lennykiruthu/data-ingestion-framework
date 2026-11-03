"""Backstop for runs that died without our code getting to write a metrics row
(out-of-memory kill, container restart, run timeout)."""

from datetime import datetime, timezone

from dagster import DefaultSensorStatus, RunFailureSensorContext, run_failure_sensor

from dagster_ingestion.landing import record_failure_if_run_unseen
from dagster_ingestion.resources import PostgresResource


@run_failure_sensor(name="record_run_failure", default_status=DefaultSensorStatus.RUNNING)
def record_run_failure(context: RunFailureSensorContext, pg: PostgresResource) -> None:
    run = context.dagster_run
    source = run.tags.get("source")
    if not source:
        return  # not one of our ingestion runs

    record = context.instance.get_run_record_by_id(run.run_id)
    started = (
        datetime.fromtimestamp(record.start_time, tz=timezone.utc)
        if record and record.start_time
        else (record.create_timestamp if record else datetime.now(timezone.utc))
    )
    keys = sorted(k.to_user_string() for k in (run.asset_selection or [])) or [run.job_name]

    with pg.connection() as conn:
        record_failure_if_run_unseen(
            conn,
            {
                "run_id": run.run_id, "source": source, "asset_key": ",".join(keys),
                "started_at": started, "finished_at": datetime.now(timezone.utc),
                "error_class": "run_crashed",
            },
        )
