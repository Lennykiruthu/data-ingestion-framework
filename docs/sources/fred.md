# Source spec card: FRED

| Field | Value |
| --- | --- |
| Source | Federal Reserve Economic Data (FRED), St. Louis Fed |
| Endpoint | `https://api.stlouisfed.org/fred/series/observations` |
| Pattern | Polling, one daily schedule (`fred_daily`, 06:00 UTC), six series |
| Auth / limits | Free API key (`FRED_API_KEY`). Documented rate limit is roughly 120 requests/minute; we make 6 per run. **Verify current limits and terms; date checked: ____** |
| Terms | Check the FRED API Terms of Use before republishing data. Some series carry third-party copyright. The dashboard shows only pipeline health, not values, and carries the attribution notice |
| Sample payload | `tests/fixtures/fred_dff_sample.json` (hand-written to FRED's documented shape; replace with a captured real response) |
| Natural key | `series_id:date`, e.g. `DFF:2026-10-20` |
| Revises records? | **Yes**, some series (e.g. CPI seasonal adjustment). A revised value has a new content hash, so it lands as a new row; "latest version" is a query |
| Hashing note | FRED stamps `realtime_start/realtime_end` with the request date on every observation, so the hash covers only series, date and value |
| Cadence + why | Daily run; the indicators publish daily (DFF, T10YIE) or monthly (GS10, CPIAUCSL, UNRATE, USREC). A daily run is cheap and the freshness rule is "stale after 2 days" |
| Backfillable? | Yes. First run requests history from 2015-01-01; `full_refresh` run config re-requests everything |
| Expected volume | Tiny: roughly 4k rows per daily series since 2015, ~130 per monthly series; well under 50 MB |
| Failure modes | Timeout/5xx (retried, 3x exponential), 429 (retried), 4xx bad key or series id (not retried, fails loudly), non-JSON (not retried). Run killed (OOM/restart): the run-failure sensor records it |
| Detected by | `ops.run_metrics` + `ops.v_source_status` (FAILING / STALE) on the dashboard |
