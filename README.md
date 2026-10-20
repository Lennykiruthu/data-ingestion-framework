---
title: "Project 1: Technical Roadmap"
description: "Architecture, data model, orchestration design, infrastructure-as-code and a week-by-week plan for a public data warehouse with a live observability dashboard."
date: 2026-10-14
draft: false
---

# Project 1: Technical Roadmap

**Last updated:** October 2026 | **Working title:** `[pick in Week 1]` (code name used below: `pdw`) | **Status:** Not started

A self-hosted warehouse that ingests public APIs and scrapers on **independent cadences per source**, orchestrated by Dagster on Postgres, deployed with Terraform and Docker Compose, with a public read-only dashboard showing how the pipelines behave. There is no business intelligence. **The product is the platform and its reliability.**

This document supersedes the earlier roadmap. Differences: local-first development, Terraform in scope, a deeper technical spec, and a shorter timeline.

---

## 1. What this proves to a hiring manager

| Signal | Evidence produced |
| --- | --- |
| Ingestion engineering | 4 sources, 4 patterns, idempotent, append-only raw layer |
| Orchestration beyond cron | Per-source schedules, a sensor, partitions, backfills, retry policy, run-queue limits |
| Data quality | Freshness, schema-drift and volume-anomaly asset checks that actually fail |
| Operability | `ops` metrics schema, public Grafana dashboard, alerting, runbook, restore drill |
| Infrastructure | Terraform-provisioned server, cloud-init bootstrap, firewall, Tailscale-only admin, rebuild-from-empty drill |
| Delivery discipline | CI on every PR, CD on merge, dated build log, scope-change log |
| Cost awareness | Memory budget for a 4 GB box, documented monthly cost |

**Interview stories this should generate:** a source that changed schema and how the check caught it; a failure I injected on purpose and how I recovered; how I made ingestion idempotent; what I did when a scraper was rate-limited; how long a full rebuild takes and how I know.

## 2. Scope, non-goals, and decisions

**In scope:** ingestion, immutable raw layer, schedules/sensors/partitions, asset checks, `ops` metrics, Grafana, Terraform, CI/CD, backups, alerting, docs.

**Non-goals (parking lot):** BI, ML, Kafka/streaming, lakehouse formats (Iceberg/Delta), multi-tenant auth, more than 4 core sources, a second dashboard tool, Ansible, CDC.

### Decisions locked (record each as a decision record in `docs/decisions/`)

| # | Decision | Choice | Revisit when |
| --- | --- | --- | --- |
| D1 | Raw storage | **Postgres JSONB** with an envelope table per source (see 5.1). Simpler to query, test and back up than Parquet-on-disk | Raw exceeds ~20 GB or query latency suffers |
| D2 | Dev model | **Local-first**: `dagster dev` on the laptop, Postgres in Docker; weekly full-Compose check | Never; the server is the same Compose file |
| D3 | Orchestrator storage | Dagster instance on **Postgres** (not SQLite) in Compose and on the server | n/a |
| D4 | Terraform | **Promoted from the parking lot** (swap logged below). Provider: `hcloud`. Server kept up most of the time; Terraform is for reproducibility, not cost-cutting | If Hetzner verification fails (see Risks) |
| D5 | Dashboard | **Grafana** reading a read-only role on the `ops` schema | n/a |
| D6 | Secrets | `.env` locally (git-ignored); **SOPS + age** for the server env file; never in cloud-init | n/a |
| D7 | Python tooling | `uv`, `ruff`, `pyright`, `pytest`, `pre-commit` | n/a |
| D8 | Admin access | **Tailscale only**; port 22 and Dagster UI never public | n/a |

### Swap log (parking lot to scope)

| Promoted | Paid for by |
| --- | --- |
| Terraform / IaC | Dropped the optional dbt staging views and the second dashboard tool; Terraform work sits in its own phase after launch so it cannot delay P1-M5 |

---

## 3. Architecture

```
                   ┌────────────────────────── Hetzner VPS (Ubuntu 24.04, 4 GB) ───────────────────────────┐
 Public sources    │                                                                                       │
 ───────────────   │   Docker Compose                                                                      │
 REST (USGS-type)──┼─►┌──────────────────┐   ┌─────────────────┐   ┌────────────────────┐                 │
 FX / indicators ──┼─►│ dagster-daemon   │──►│   Postgres 16   │◄──│ grafana (read-only │──► Caddy :443 ──┼──► public
 Scraper (HTML)  ──┼─►│ (schedules,      │   │ raw.*  ops.*    │   │ role on ops.*)     │    (TLS)        │    dashboard
 File/event source─┼─►│  sensors, runs)  │   │ dagster.* (meta)│   └────────────────────┘                 │
                   │  └──────────────────┘   └─────────────────┘                                          │
                   │  ┌──────────────────┐            ▲                                                   │
                   │  │dagster-webserver │            │ nightly pg_dump ─► restic ─► S3-compatible bucket │
                   │  │ 127.0.0.1:3000   │            │                    (different provider)           │
                   │  └────────▲─────────┘                                                                │
                   └───────────┼──────────────────────────────────────────────────────────────────────────┘
                               │ Tailscale (admin only)
 GitHub Actions ───────────────┘  CI: lint/type/test/tf validate/build   CD: push image ► deploy over Tailscale
 Terraform (hcloud) ──► server, firewall, primary IP, volume, SSH key   (apply is manual, never automatic)
```

Flow of one ingestion run: **schedule/sensor tick → Dagster run → asset fetches from source → per-record hash → `INSERT ... ON CONFLICT DO NOTHING` into `raw.<source>` → asset emits metadata (rows fetched/inserted, bytes, latency, HTTP status, schema hash) → asset checks evaluate → run-status sensor writes `ops.run_metrics` → Grafana reads `ops` views.**

## 4. Stack

| Layer | Choice | Notes |
| --- | --- | --- |
| Language | Python 3.12 | Pin in `.python-version` |
| Orchestration | Dagster (pin an exact version; check docs for current scaffolding commands, they have changed across releases) | Webserver + daemon, QueuedRunCoordinator |
| DB | Postgres 16 | Schemas: `raw`, `ops`, plus Dagster's own tables |
| HTTP | `httpx` with timeouts, conditional GET (ETag), `Retry-After` handling | No bare `requests.get` |
| Scraping | `selectolax` or `lxml`/`BeautifulSoup`; `urllib.robotparser` for robots.txt | Fixtures for tests |
| Validation | `pydantic` models at the boundary (log violations, **still store raw**) | |
| DB driver | `psycopg` 3, plain SQL, no ORM | |
| Dashboard | Grafana | Anonymous Viewer role, public only through Caddy |
| Reverse proxy | Caddy | Automatic TLS |
| IaC | Terraform (OpenTofu is a drop-in alternative) with `hetznercloud/hcloud` | |
| CI/CD | GitHub Actions | |
| Backups | `pg_dump` + `restic` to an S3-compatible bucket | Different provider from the server |
| Admin network | Tailscale | |

## 5. Repo layout

```
pdw/
├── pyproject.toml            # uv-managed; dagster pinned
├── uv.lock
├── .env.example              # committed; .env is git-ignored
├── docker-compose.yml        # base (local + server)
├── docker-compose.prod.yml   # overrides: Caddy, Grafana, memory limits, restart policies
├── Dockerfile
├── dagster.yaml              # instance config (Postgres storage, run queue)
├── workspace.yaml
├── src/pdw/
│   ├── definitions.py        # Definitions(assets, schedules, sensors, checks, resources)
│   ├── resources/            # PostgresResource, HttpClientResource, Settings
│   ├── sources/
│   │   ├── usgs/             # assets.py, parse.py, schemas.py
│   │   ├── fx/
│   │   ├── <scraper>/
│   │   └── <sensor_source>/
│   ├── checks/               # freshness.py, schema_drift.py, volume.py
│   ├── ops/                  # metrics sensor, alert sensor
│   └── common/               # hashing.py, backoff.py, robots.py, time.py
├── sql/
│   ├── 001_schemas_roles.sql
│   ├── 002_raw_envelope.sql
│   └── 003_ops.sql
├── tests/
│   ├── fixtures/             # recorded API JSON and HTML pages
│   ├── unit/
│   └── integration/          # Postgres service container
├── infra/
│   ├── terraform/            # main.tf variables.tf outputs.tf cloud-init.yaml.tftpl
│   ├── caddy/Caddyfile
│   ├── grafana/              # provisioning: datasource + dashboard JSON (committed)
│   └── scripts/              # backup.sh, restore.sh, bootstrap.sh
├── .github/workflows/        # ci.yml  deploy.yml  tf-plan.yml
└── content/ (or docs/)       # docs, build-log, planning, per the planning guide
```

Commit the Grafana dashboard JSON and provisioning files. A dashboard that exists only in a UI is not reproducible.

---

## 6. Data model

### 6.1 Raw envelope (one table per source, identical shape)

```sql
create schema if not exists raw;

create table raw.usgs_earthquakes (
    _id            bigint generated always as identity primary key,
    _source        text        not null default 'usgs_earthquakes',
    _run_id        text        not null,                -- Dagster run id
    _ingested_at   timestamptz not null default now(),
    _request_url   text,
    _payload_hash  bytea       not null,                -- sha256 of canonical JSON of the record
    _record_key    text        not null,                -- natural key from the source, if any
    payload        jsonb       not null
);

-- Idempotency: the same record content is stored once, even if the run is retried.
create unique index raw_usgs_dedupe on raw.usgs_earthquakes (_record_key, _payload_hash);
create index raw_usgs_ingested on raw.usgs_earthquakes (_ingested_at);
```

Design rules:
- **Append-only.** The application role has `INSERT, SELECT` on `raw.*` and no `UPDATE/DELETE`.
- **Changed records become new rows.** Many feeds revise events after publishing (a record with the same key but different content). Because the dedupe key is `(_record_key, _payload_hash)`, a revision inserts a new row. "Latest version" is a query, not a mutation.
- **Canonical hashing.** Hash `json.dumps(record, sort_keys=True, separators=(",", ":"))` so key order and whitespace never create false changes.
- **Large tables:** declare range partitioning on `_ingested_at` (monthly) from day one for the high-frequency source. It is cheap now and painful later.

### 6.2 `ops` schema

```sql
create schema if not exists ops;

create table ops.source_config (
    source              text primary key,
    expected_interval   interval not null,      -- cadence used for freshness
    owner_note          text
);

create table ops.run_metrics (
    id              bigint generated always as identity primary key,
    run_id          text not null,
    source          text not null,
    asset_key       text not null,
    status          text not null,              -- SUCCESS | FAILURE
    started_at      timestamptz not null,
    finished_at     timestamptz not null,
    latency_ms      integer,
    rows_fetched    integer,
    rows_inserted   integer,
    bytes_fetched   bigint,
    http_status     integer,
    schema_hash     text,
    error_class     text,
    unique (run_id, asset_key)
);

create table ops.schema_registry (
    source        text not null,
    schema_hash   text not null,
    first_seen_at timestamptz not null default now(),
    sample        jsonb,
    primary key (source, schema_hash)
);

create table ops.storage_snapshot (
    taken_at   timestamptz not null default now(),
    relation   text not null,
    row_estimate bigint,
    total_bytes bigint,
    primary key (taken_at, relation)
);
```

### 6.3 Views the dashboard reads

```sql
create view ops.v_source_freshness as
select c.source,
       max(m.finished_at) filter (where m.status = 'SUCCESS')              as last_success_at,
       now() - max(m.finished_at) filter (where m.status = 'SUCCESS')      as staleness,
       c.expected_interval,
       (now() - max(m.finished_at) filter (where m.status = 'SUCCESS')) > 2 * c.expected_interval as is_stale
from ops.source_config c
left join ops.run_metrics m using (source)
group by c.source, c.expected_interval;

create view ops.v_success_rate_7d as
select source,
       count(*)                                          as runs,
       100.0 * count(*) filter (where status = 'SUCCESS') / count(*) as success_pct
from ops.run_metrics
where started_at > now() - interval '7 days'
group by source;
```

Grafana connects as `grafana_ro`, granted `USAGE` on `ops` and `SELECT` on **views only**. Raw payloads are never exposed.

---

## 7. Orchestration design

### 7.1 Principles

1. **One asset per source "landing" step.** Keep assets thin: fetch, hash, insert, emit metadata. Parsing lives in pure functions in `parse.py` so it is unit-testable without I/O.
2. **Resources for everything external** (`PostgresResource`, `HttpClientResource`, `Settings`). Laptop and server differ only by environment variables.
3. **Idempotent and re-runnable.** Any run can be repeated or backfilled without creating duplicates.
4. **Timezones explicit.** Use `execution_timezone="Africa/Nairobi"` on schedules where local time matters, UTC in storage.
5. **Protect the box.** `QueuedRunCoordinator` with `max_concurrent_runs: 3` so a backfill cannot exhaust 4 GB of RAM.

### 7.2 Schedules and sensors (indicative; finalise in the source spec cards)

| Source type | Trigger | Cadence | Notes |
| --- | --- | --- | --- |
| Polling REST (event feed) | Schedule | `*/5 * * * *` | High frequency; monthly-partitioned raw table |
| FX / reference rates | Schedule + `DailyPartitionsDefinition` | Daily (or hourly) | Backfill history through partitions |
| Market data / scraper | Schedule | Weekdays after close (Nairobi time) | Rate-limited; trading-calendar aware |
| Slow indicators | Schedule | Monthly | Paginated; large backfill |
| File / publication source | **Sensor** | Poll every N minutes, cursor on ETag/Last-Modified/hash | Triggers a run only when new content appears |

Use a Dagster **cursor** on the sensor so it never re-triggers for content it has already seen.

### 7.3 Retry and HTTP behaviour

- `RetryPolicy(max_retries=3, delay=..., backoff=Backoff.EXPONENTIAL, jitter=Jitter.PLUS_MINUS)` on assets that call the network.
- `httpx` timeouts on **connect and read**; honour `Retry-After`; send an identifying `User-Agent` with a contact address; use conditional requests (`If-None-Match`) where supported.
- Distinguish **retryable** (timeouts, 429, 5xx) from **non-retryable** (4xx, parse failures). Parse failures should fail loudly, not retry.
- Scrapers: fetch `robots.txt` and check allowed paths in code, enforce a minimum delay between requests, never collect personal data, and commit `docs/compliance.md` recording the ToS check and date.

### 7.4 Asset checks (the "data quality" story)

| Check | Applies to | Logic | Fails when |
| --- | --- | --- | --- |
| Freshness | All 4 sources | Last successful materialisation within `2 × expected_interval` (use Dagster's built-in freshness checks where available; confirm the current API in the docs) | Source stale |
| Schema drift | >= 2 sources | Compute a hash of the sorted set of JSON key paths per record batch; compare to `ops.schema_registry`; unseen hash raises the check and inserts the new hash with a sample | New/changed fields appear |
| Volume anomaly | >= 1 source | `rows_fetched` of latest run vs median of last 7 runs; flag outside a configurable ratio | Sudden drop or spike |
| Row integrity | All | `rows_inserted <= rows_fetched`, no null `_record_key` | Pipeline logic bug |

Set check severity deliberately: schema drift is `WARN` (data still lands, a human decides); freshness and integrity are `ERROR`.

### 7.5 Run metrics: `ops.run_metrics`

Assets add metadata at the end of each run (`rows_fetched`, `rows_inserted`, `bytes_fetched`, `latency_ms`, `http_status`, `schema_hash`). A **run-status sensor** (SUCCESS and FAILURE) reads run stats and asset metadata from the instance and upserts into `ops.run_metrics`. This captures failures, which asset-level logging would miss. A second run-failure sensor sends the alert (section 12).

Build this sensor in **Week 5, before the dashboard.** You need the data accumulating during the soak test.

---

## 8. Sources

Choose **4** in Week 1 and freeze them. Fill one **spec card** per source *before* writing code:

```
Source:            Endpoint / page:
Pattern:           polling | scrape | sensor | paginated-backfill
Auth / limits:     key? rate limit? robots/ToS note + date checked:
Sample payload:    (commit to tests/fixtures/)
Natural key:       field(s) that identify a record
Revises records?   yes/no (determines hash vs key strategy)
Cadence + why:     
Backfillable?      yes (how far) | no (data lost if we miss it)
Expected volume:   rows/day x avg bytes = GB/month
Failure modes:     what breaks, how detected
```

Required mix: one high-frequency polling API, one scraper, one sensor-driven source, one slow-cadence/paginated source. **Include at least one Kenyan or African source** (matches the long-term goal on your About page) and, given your background, one market-data source if its terms allow. Verify availability and terms yourself before committing; sources and terms change.

Mark each source **backfillable or not**. For non-backfillable live feeds, any downtime is permanent data loss, which is the argument for keeping the server up.

---

## 9. Local development

### 9.1 Environment

```bash
# one-time
uv init pdw && cd pdw
uv add dagster dagster-webserver dagster-postgres dagster-docker httpx psycopg[binary] pydantic selectolax
uv add --dev pytest ruff pyright pre-commit
# scaffold the Dagster project per the CURRENT Dagster docs (commands vary by version)

cp .env.example .env
docker compose up -d db            # only Postgres in Docker
export DAGSTER_HOME=$PWD/.dagster_home && mkdir -p $DAGSTER_HOME
uv run dagster dev -m pdw.definitions    # UI on localhost:3000, daemon included
```

Weekly: `docker compose up --build` runs the whole stack as it will run on the server.

### 9.2 `dagster.yaml` (instance on Postgres, queued runs)

```yaml
storage:
  postgres:
    postgres_db:
      username: { env: DAGSTER_PG_USER }
      password: { env: DAGSTER_PG_PASSWORD }
      hostname: { env: DAGSTER_PG_HOST }
      db_name:  { env: DAGSTER_PG_DB }
      port: 5432

run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 3
```

### 9.3 `docker-compose.yml` (sketch; verify against your versions)

```yaml
services:
  db:
    image: postgres:16
    environment:
      POSTGRES_USER: ${PG_USER}
      POSTGRES_PASSWORD: ${PG_PASSWORD}
      POSTGRES_DB: ${PG_DB}
    volumes:
      - ${PG_DATA_DIR:-./.data/pg}:/var/lib/postgresql/data
      - ./sql:/docker-entrypoint-initdb.d:ro
    ports: ["127.0.0.1:5432:5432"]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${PG_USER}"]
      interval: 5s
      retries: 10
    mem_limit: 1g
    restart: unless-stopped

  dagster-webserver:
    build: .
    command: ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "workspace.yaml"]
    env_file: .env
    environment: { DAGSTER_HOME: /opt/dagster/home }
    ports: ["127.0.0.1:3000:3000"]        # never published publicly
    depends_on: { db: { condition: service_healthy } }
    mem_limit: 700m
    restart: unless-stopped

  dagster-daemon:
    build: .
    command: ["dagster-daemon", "run", "-w", "workspace.yaml"]
    env_file: .env
    environment: { DAGSTER_HOME: /opt/dagster/home }
    depends_on: { db: { condition: service_healthy } }
    mem_limit: 900m
    restart: unless-stopped
```

`docker-compose.prod.yml` adds Caddy (80/443) and Grafana (internal port only), plus a 2 GB swapfile on the host as a safety net.

**Server memory budget (4 GB):** Postgres ~1 GB, daemon + run workers ~0.9 GB, webserver ~0.7 GB, Grafana ~0.3 GB, Caddy and OS ~0.5 GB, headroom ~0.6 GB. If you cannot stay inside this, reduce `max_concurrent_runs` before buying a bigger box.

### 9.4 Local caveats

- Schedules only fire while the laptop is awake. Local data has gaps, so **do not count local runs in public metrics**; the soak clock starts on the server.
- Make every source backfillable or note it as "not recoverable" in its spec card.

---

## 10. Server and security

| Control | Implementation |
| --- | --- |
| Public surface | Only 80/443 (Caddy to Grafana). Nothing else |
| Admin access | Tailscale only. Port 22 closed in the firewall. Keep a documented recovery path (Hetzner web console, or temporarily allow 22 from your IP in Terraform) |
| Dagster UI | Bound to `127.0.0.1`; reach via Tailscale (`tailscale serve` or an SSH tunnel over Tailscale) |
| Postgres | Bound to `127.0.0.1`; never public |
| DB roles | `pdw_app` (insert/select on `raw`, write on `ops`), `grafana_ro` (select on `ops` views only), no superuser use by the app |
| Host | SSH keys only, `unattended-upgrades` on, `ufw` or Hetzner firewall as the single source of truth |
| Secrets | SOPS + age encrypted `.env.prod` in the repo; decrypt at deploy time. Never in `user_data` (readable via the metadata service and console) |
| Grafana | Anonymous Viewer, no sign-up, no editing, admin password from secrets |

---

## 11. CI/CD (GitHub Actions)

| Workflow | Trigger | Steps |
| --- | --- | --- |
| `ci.yml` | Every PR + push | `ruff check`, `ruff format --check`, `pyright`, `pytest` (unit + integration with a Postgres service container), `docker build`, `terraform fmt -check` + `terraform validate` |
| `tf-plan.yml` | PR touching `infra/terraform` | `terraform plan` with a **read-only** Hetzner token, plan posted as a PR comment. **No auto-apply** |
| `deploy.yml` | Push to `main` after CI passes | Build and push image to GHCR, connect to the tailnet with an ephemeral Tailscale auth key (GitHub Action), `ssh` to the server, decrypt env, `docker compose pull && docker compose up -d`, run a smoke check (Dagster `/server_info` or a materialisation of a canary asset) |

Add branch protection on `main`: required status checks, no direct pushes.

---

## 12. Terraform

### 12.1 Goals

Reproducible server from an empty account, firewall and IP defined as code, data surviving a server rebuild, and a **measured rebuild time**. Terraform here is for *reproducibility and review*, not for cost-cutting: the production server stays up.

### 12.2 Layout

```
infra/terraform/
├── main.tf               # provider, resources
├── variables.tf          # server_type, datacenter, ssh key path, tailscale key (sensitive)
├── outputs.tf            # server IPv4, volume id
├── cloud-init.yaml.tftpl # bootstrap: packages, docker, tailscale, mount volume, clone repo
└── terraform.tfvars.example
```

### 12.3 Sketch (validate with `terraform validate`; check current server type names with `hcloud server-type list`)

```hcl
terraform {
  required_version = ">= 1.6"
  required_providers {
    hcloud = { source = "hetznercloud/hcloud", version = "~> 1.48" }
  }
  # Remote state: pick one and record it as a decision (S3-compatible bucket at a
  # different provider, or a hosted Terraform backend). Do not leave state only on a laptop.
}

provider "hcloud" { token = var.hcloud_token }

resource "hcloud_ssh_key" "admin" {
  name       = "pdw-admin"
  public_key = file(var.ssh_public_key_path)
}

resource "hcloud_firewall" "pdw" {
  name = "pdw"
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "80"
    source_ips = ["0.0.0.0/0", "::/0"]
  }
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "443"
    source_ips = ["0.0.0.0/0", "::/0"]
  }
  # No inbound 22: administration happens over Tailscale.
}

# Keep the IP across server rebuilds so DNS never changes.
resource "hcloud_primary_ip" "v4" {
  name          = "pdw-v4"
  type          = "ipv4"
  assignee_type = "server"
  datacenter    = var.datacenter
  auto_delete   = false
  lifecycle { prevent_destroy = true }
}

# Postgres data lives on a volume so it survives server replacement.
resource "hcloud_volume" "pgdata" {
  name     = "pdw-pgdata"
  size     = 20
  location = var.location
  format   = "ext4"
  lifecycle { prevent_destroy = true }
}

resource "hcloud_server" "pdw" {
  name         = "pdw-1"
  server_type  = var.server_type          # verify the 4 GB type name
  image        = "ubuntu-24.04"
  datacenter   = var.datacenter
  ssh_keys     = [hcloud_ssh_key.admin.id]
  firewall_ids = [hcloud_firewall.pdw.id]

  public_net {
    ipv4_enabled = true
    ipv4         = hcloud_primary_ip.v4.id
    ipv6_enabled = true
  }

  user_data = templatefile("${path.module}/cloud-init.yaml.tftpl", {
    tailscale_authkey = var.tailscale_authkey   # sensitive, short-lived, single-use
    repo_url          = var.repo_url
  })

  lifecycle { ignore_changes = [user_data] }    # cloud-init only runs on first boot
}

resource "hcloud_volume_attachment" "pgdata" {
  volume_id = hcloud_volume.pgdata.id
  server_id = hcloud_server.pdw.id
  automount = true
}
```

Cloud-init does only bootstrap: create a non-root user, install Docker and Tailscale, `tailscale up` with the single-use key, mount/bind the volume at the Postgres data path, clone the repo, enable `unattended-upgrades`, add a swapfile. **It never contains application secrets.** Secrets arrive through the SOPS-encrypted env file at first deploy.

### 12.4 Practices

- **Plan on PR, apply by hand** (or a manually approved `workflow_dispatch`). Never auto-apply infrastructure.
- **`prevent_destroy`** on the volume and primary IP.
- Use a separate **read-only** token in CI for plans, and a read/write token only on your machine.
- Set a **billing alert** and tag every resource with a `project=pdw` label so an orphan is easy to find.
- The rebuild drill uses a **second, temporary environment** (variable `env=drill`) for about an hour, so production is untouched and the cost is cents. Delete it afterwards.

### 12.5 Rebuild drill (milestone P1-M6)

1. `terraform apply -var env=drill` into a clean state.
2. Time from `apply` to a green smoke check on the new box, **with no manual steps** other than the Tailscale key and SOPS key.
3. Restore the latest `restic` backup into it and verify row counts against production for a sample of tables.
4. Record: time-to-running (RTO), data age (RPO), every manual step you had to do. Each manual step becomes a script or a documented gap.
5. `terraform destroy` the drill environment.

---

## 13. Testing strategy

| Level | What | How |
| --- | --- | --- |
| Unit | Parsers, hashing, schema-hash, volume-anomaly math, robots checks | Pure functions + fixtures in `tests/fixtures/`. No network |
| Integration | Insert idempotency (run twice, same rows), role permissions (app cannot UPDATE/DELETE raw), views | Postgres service container in CI |
| Dagster | Assets and checks end to end | `dagster.materialize([...], resources={fakes})` with fake HTTP client |
| Contract | A recorded real payload still parses; drift check fires on a mutated payload | Fixture diff test |
| Smoke | Deployed stack is alive | Canary asset materialisation post-deploy |
| Resilience | Injected failure is detected and recovered | Documented game-day in the build log (see P1-M4) |

Rule: **every bug you fix gets a regression test or a fixture.**

---

## 14. Backups, alerting, retention

**Backups**
- Nightly `pg_dump -Fc` of the application databases, then `restic backup` to an S3-compatible bucket at a **different provider** from the server. Retention: 7 daily, 4 weekly.
- Dagster metadata is rebuildable and lower priority; `raw` and `ops` are what matter.
- **A backup is not real until restored.** The drill in 12.5 proves it. WAL archiving / PITR goes to the parking lot.

**Alerting**
- Run-failure sensor sends a message (Telegram, ntfy, or email webhook) containing source, asset, error class, and a link.
- An external uptime check on the public dashboard URL.
- Disk-usage alert at 80% (volume and root).
- Stale-source alert driven by `ops.v_source_freshness.is_stale`.

**Retention and cost**
- Track growth in `ops.storage_snapshot` (daily). Project months until the volume is full and write it in the runbook.
- Decide per source: keep all versions forever, or compact versions older than N days into a summary. Document the decision.

---

## 15. Timeline and milestones

Assumes **10-12 hours/week**: one 6-7 hour Saturday plus 2-3 evenings, Sunday off, planned at 70% capacity. Week 1 starts Monday 19 October 2026. Weeks 10-11 (21 Dec - 3 Jan) are **half capacity**.

| ID | Milestone | Done when (measurable) | Target (Sat) |
| --- | --- | --- | --- |
| **P1-M0** | Local skeleton | Repo + `uv` env + pre-commit. Postgres in Compose. `dagster dev` runs. `hello_world` asset materialises and a `pytest` proves it. CI (ruff, pyright, pytest) green on a PR. 4 sources chosen with spec cards committed. Decisions D1-D8 written down | 24 Oct |
| **P1-M1** | Two sources, containerised | USGS-style polling source and a second source, different schedules, landing in `raw.*` append-only with the envelope. **Re-running a run inserts 0 new rows** (tested). Retry policy in place. Full stack runs via `docker compose up --build`. 48 h unattended locally on `dagster dev` is not required; correctness is | 7 Nov |
| **P1-M2** | On the server | Server provisioned (manually or via first Terraform pass), Tailscale-only admin, Compose stack running, CD deploys on merge to `main`, canary smoke check passes. **Soak clock starts** | 14 Nov |
| **P1-M3** | Four sources + checks + metrics | Scraper (with compliance note + robots check in code) and sensor source added. Freshness on all 4, schema-drift on >= 2, volume-anomaly on >= 1, integrity on all. `ops.run_metrics` populated by the run-status sensor. Alert sensor live | 28 Nov |
| **P1-M4** | Soak, backfill, failure drill | **7 consecutive days unattended** on the server with **>= 95% run success**. >= 1 partitioned backfill demonstrated (e.g. 30 days). **One injected failure** (source outage or schema change) detected by a check, alerted, recovered, written up. Grafana dashboard v1 with **>= 4 panels** built during the soak | 5 Dec |
| **P1-M5** | Public launch | Dashboard at a public HTTPS URL (read-only role, Dagster private). README + architecture diagram + CI badge. Case study on the site. Homepage updated. **Start applying** | 12 Dec |
| **P1-M6** | Terraform + rebuild drill | Terraform manages server, firewall, primary IP, volume. `tf-plan` on PRs. **Rebuild-from-empty drill completed** in a temporary environment with measured RTO and a restore check | 19 Dec |
| **P1-M7** | Production-ready (half-capacity weeks) | Nightly restic backup with **a tested restore**. Alerting covers failure, staleness, disk, uptime. Monthly cost documented. >= 5 decision records. Runbook: deploy, restore, add-a-source, rotate secrets | 9 Jan |
| **P1-M8** | Positioned | Case study updated with 4+ weeks of real numbers. 3-5 minute walkthrough video. One blog post on the hardest problem. Site links fixed, both stacks listed on About page. CV bullets filled from the scoreboard | 16 Jan |

**Hours budget (approx.):** M0 11 | M1 22 | M2 11 | M3 22 | M4 11 | M5 11 | M6 11 | M7 ~20 (holiday-adjusted) | M8 11. Total about 130 hours.

**Two-week rule:** any milestone 14 days past target triggers a scope-change entry; scope shrinks, the date does not move. Pre-agreed shrink options:

| Milestone | First cut | Second cut |
| --- | --- | --- |
| M1 | Second source becomes a simpler API | Drop the weekly full-Compose check |
| M2 | Manual server provisioning (Terraform waits for M6) | CD becomes a one-command manual deploy script |
| M3 | Source #4 becomes a reserve source (3 sources) | Volume-anomaly check dropped |
| M4 | Dashboard v1 with 3 panels | Backfill demonstrated on 7 days, not 30 |
| M6 | Rebuild drill without restore verification | Terraform manages server + firewall only |
| M7 | Skip uptime monitor | Alerts to email only |

Never cut: idempotency test, freshness checks, the soak test, the restore test.

### Career checkpoints

- [ ] **Week 8 (P1-M5):** apply to 3-5 roles/week, tailoring the top CV bullet. Expect January to pick up.
- [ ] **Week 9:** publish a short post about the hardest bug so far; message 5 data engineers or hiring managers with the live link.
- [ ] **Week 13:** refresh CV and LinkedIn with the final scoreboard; follow up on stale applications.

---

## 16. First four Saturdays, in detail

### Saturday 1 (24 Oct): skeleton, tests, CI
- [ ] Create repo, `uv init`, pin Python and Dagster, add pre-commit (ruff, pyright)
- [ ] Postgres in Compose with `sql/001_schemas_roles.sql` creating `raw`, `ops`, and roles
- [ ] Scaffold the Dagster project per current docs; `Definitions` in `definitions.py`
- [ ] `hello_world` asset writing one row into `ops`; a pytest materialises it against Postgres
- [ ] GitHub Actions `ci.yml` with a Postgres service container
- [ ] Write the four spec cards (even rough) and decisions D1-D8
- **Done when:** CI is green on a PR and `dagster dev` shows the asset.

### Saturday 2 (31 Oct): first source, end to end
- [ ] Capture a real payload into `tests/fixtures/`
- [ ] `parse.py` (pure), `hashing.py` (canonical hash), unit tests
- [ ] `raw.<source>` DDL with the unique index; app role without UPDATE/DELETE
- [ ] Asset: fetch via `HttpClientResource` (timeouts, retry policy), insert with `ON CONFLICT DO NOTHING`, emit metadata
- [ ] Schedule with the chosen cron and `execution_timezone`
- [ ] Integration test: run twice, second run inserts 0 rows
- **Done when:** the idempotency test passes and a schedule tick produces a materialisation.

### Saturday 3 (7 Nov): second source and full containerisation
- [ ] Second source on a different cadence/pattern (partitioned if it has history)
- [ ] `Dockerfile`, `docker-compose.yml`, `dagster.yaml` on Postgres, `QueuedRunCoordinator`
- [ ] `docker compose up --build` runs webserver + daemon + db; schedules fire in-container
- [ ] First two build-log entries written; **P1-M1 checked off**
- **Done when:** the whole stack comes up from a clean clone with one command.

### Saturday 4 (14 Nov): server and CD
- [ ] Provision VPS (manually is fine this week), install Docker, Tailscale, lock the firewall to 80/443
- [ ] SOPS + age set up; `.env.prod` encrypted in the repo
- [ ] `deploy.yml`: image to GHCR, deploy over Tailscale, smoke check
- [ ] Merge a trivial change and watch it deploy
- **Done when:** merging to `main` updates the server with no manual steps. **P1-M2 reached; the soak clock starts.**

---

## 17. Scoreboard (numbers I quote)

| Metric | Target | Actual |
| --- | --- | --- |
| Sources in production | 4 | |
| Distinct schedules + sensors | 4+ | |
| Runs per day | | |
| 30-day run success rate | >= 95% | |
| Median freshness lag per source | | |
| Rows ingested to date | | |
| Storage used / growth per week | | |
| Automated checks | >= 7 | |
| Failures injected and recovered | >= 1 | |
| Rebuild-from-empty time (RTO) | | |
| Restore verified (date, RPO) | | |
| Monthly infrastructure cost | | |
| Tests / CI pass rate | | |

**CV bullet template:**

> Built and operate a self-hosted data platform (Dagster, Postgres, Docker, Terraform, Grafana) ingesting **[N] public sources** on **[N] independent schedules and sensors** with idempotent, append-only raw storage; **[X]% run success over [N] days**, freshness/schema-drift/volume checks, CI/CD to a **$[X]/month** VPS, and a **[N]-minute** measured rebuild-from-scratch via Terraform.

---

## 18. Risks

| Risk | Mitigation |
| --- | --- |
| Hetzner signup/verification fails | Don't block on it: local-first until M2. Fallback host paid by M-Pesa. Terraform then targets whichever provider has an official provider; otherwise M6 becomes an idempotent bootstrap script + the same drill |
| Scope creep | Parking lot + swap rule; 4 sources frozen in M0 |
| Source changes or blocks | Reserve source list; drift check; scraper compliance note |
| 4 GB exhausted by backfills | Run queue limit, container memory limits, swapfile |
| Locked out after closing port 22 | Tailscale health in smoke check; documented console recovery |
| Terraform state loss or orphaned resources | Remote state; `project=pdw` labels; billing alert |
| Non-backfillable data lost during downtime | Server kept up; downtime recorded in the dashboard annotations |
| Work weeks at the trading job | 70% planning; two-week rule shrinks scope, never dates |
| Holiday weeks | Half-capacity plan; M7 deliberately low-risk work |

## 19. Status log

- [ ] P1-M0 Local skeleton
- [ ] P1-M1 Two sources, containerised
- [ ] P1-M2 On the server
- [ ] P1-M3 Four sources + checks + metrics
- [ ] P1-M4 Soak, backfill, failure drill
- [ ] P1-M5 Public launch (start applying)
- [ ] P1-M6 Terraform + rebuild drill
- [ ] P1-M7 Production-ready
- [ ] P1-M8 Positioned

Slips so far: see `scope-changes.md`.
