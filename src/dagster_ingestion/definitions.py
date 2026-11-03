from dagster import Definitions, EnvVar

from dagster_ingestion.resources import FredResource, PostgresResource
from dagster_ingestion.sensors import record_run_failure
from dagster_ingestion.sources.fred.assets import fred_job, fred_schedule, raw_fred

defs = Definitions(
    assets=[raw_fred],
    jobs=[fred_job],
    schedules=[fred_schedule],
    sensors=[record_run_failure],
    resources={
        # EnvVar is resolved when a run starts, so a missing variable fails that run
        # (visibly) instead of crashing the whole code location at import time.
        "pg": PostgresResource(
            host=EnvVar("APP_DB_HOST"),
            dbname=EnvVar("PG_DB"),
            user=EnvVar("APP_DB_USER"),
            password=EnvVar("APP_DB_PASSWORD"),
        ),
        "fred": FredResource(api_key=EnvVar("FRED_API_KEY")),
    },
)
