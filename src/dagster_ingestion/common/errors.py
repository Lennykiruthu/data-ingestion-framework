"""One failure type for everything that can go wrong while ingesting.

`category` is a small closed vocabulary stored in ops.run_metrics.error_class and turned
into plain English by the ops.v_recent_failures view:
    timeout | rate_limited | http_5xx | http_4xx | parse_error | db_error | run_crashed | unknown
"""

from dagster import Failure


class IngestionFailure(Failure):
    def __init__(self, description: str, *, category: str, retryable: bool):
        super().__init__(
            description=description,
            metadata={"error_class": category},
            allow_retries=retryable,  # False -> Dagster's RetryPolicy will not retry
        )
        self.category = category
