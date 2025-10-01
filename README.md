# Pipeline2025

# ApiIngestor

Config‑driven HTTP → DataFrame → S3 ingestor with retries, pagination, backfills, and optional per‑row link expansion.

> Built around `requests.Session`, `pandas`, and `boto3`. Configured via YAML with `${ENV_VAR}` expansion.

---

## Highlights

- **Session-level retries** via `urllib3.Retry` mounted on `requests.Session`.
- **Pagination**: `none`, `page`, `cursor` (token or chained id), `link-header`, **Salesforce** (`done`/`nextRecordsUrl`).
- **Backfills**: generic **date windows**, **Salesforce SOQL windowing**, or **cursor-bounded** with stop conditions.
- **Link expansion**: follow URLs in each row; inherits auth/headers/verify/proxies from the session.
- **Parsers**: JSON (with record path + key dropping) or CSV.
- **Output (required)**: writes **CSV / JSONL / Parquet** to **S3** with optional gzip (CSV) and Parquet compression.
- **Safety**: only whitelisted `requests.get` kwargs are passed; sensitive headers/params are **redacted** in logs.

---

## Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install pandas requests urllib3 boto3 pytest behave responses pyarrow fastparquet
```

> Parquet requires either `pyarrow` or `fastparquet`.

---

## Quick start

```python
from utils.api_ingestor import ApiIngestor
import logging, yaml
from datetime import date

log = logging.getLogger("ingestor")
log.setLevel(logging.INFO)

with open("config/ingestor.yaml") as f:
    cfg = yaml.safe_load(f)

ing = ApiIngestor(cfg, log)

# One-off pull
meta = ing.run_once(table_name="events_api", env_name="qa")
print(meta)

# Date-window backfill
meta = ing.run_backfill(
    table_name="marketing_reports", env_name="prod",
    start=date(2025, 1, 1), end=date(2025, 1, 31)
)
print(meta)
```

**Environment variables** inside the YAML like `${SF_TOKEN}` are expanded at runtime.
Missing env vars are **left as-is** (e.g., `${MISSING}`) so you can detect them.

---

## Configuration (YAML)

Your code now supports:

- Table‑level `path`, `headers`, `params`, and `pagination` that **merge/override** globals.
- Global `output` sink (S3) that is **required**; tables can override `output`.
- Session defaults are **applied to the session** (headers/auth/proxies/verify) so **link expansion inherits them**.
- Link expansion **does not inherit** the base `json_record_path` by default; set `link_expansion.json_record_path` if needed.

A complete example (trim to what you use):

```yaml
envs:
  dev:
    base_url: "https://api.vendor-a.com/"
  qa:
    base_url: "https://finance.example.com/api/"
  prod:
    base_url: "https://yourinstance.my.salesforce.com/"

apis:
  request_defaults:
    headers:
      Authorization: "Bearer ${VENDOR_A_TOKEN}"
      Accept: "application/json"
    timeout: 60
    verify: true

  # Default path (used by SF tables unless overridden)
  path: "services/data/v61.0/query"

  retries:
    total: 4
    backoff_factor: 0.5
    status_forcelist: [429, 500, 502, 503, 504]
    allowed_methods: ["GET"]

  pagination:
    mode: "salesforce"
    done_path: "done"
    next_url_path: "nextRecordsUrl"
    clear_params_on_next: true
    max_pages: 20000

  link_expansion:
    enabled: true
    url_fields: ["detail_url", "links.self"]
    json_record_path: null
    per_request_delay: 0.0

  # REQUIRED global sink (tables may override)
  output:
    format: parquet        # csv | parquet | jsonl
    write_empty: true
    s3:
      bucket: "my-ingest-bucket"
      prefix: "{env}/{table}/{today:%Y/%m/%d}"
      region_name: "us-east-1"
      compression: "snappy"  # only for parquet

  # --- Salesforce + SOQL windowed backfill ---
  marketing_reports:
    headers:
      Authorization: "Bearer ${SF_TOKEN}"
    parse:
      type: "json"
      json_record_path: "records"
      json_drop_keys_any_depth: ["attributes"]
    backfill:
      enabled: true
      strategy: "soql_window"
      date_field: "LastModifiedDate"
      date_format: "%Y-%m-%dT%H:%M:%SZ"
      window_days: 7
      soql_template: |
        SELECT Id, Name, LastModifiedDate
        FROM Account
        WHERE {date_field} >= {start}
          AND {date_field} <  {end}
        ORDER BY {date_field} ASC

  # --- Page pagination + date backfill (non-SF) ---
  events_api:
    path: "v1/events"
    headers:
      Authorization: "Bearer ${FINANCE_TOKEN}"
    parse:
      type: "json"
      json_record_path: "data.items"
    pagination:
      mode: "page"
      page_param: "page"
      start_page: 1
      page_size_param: "page_size"
      page_size_value: 500
      max_pages: 10000
    backfill:
      enabled: true
      strategy: "date"
      start_param: "start_date"
      end_param: "end_date"
      date_format: "%Y-%m-%d"
      window_days: 3

  # --- Cursor pagination (opaque token) + cursor backfill ---
  activity_feed:
    path: "v1/activity"
    parse:
      type: "json"
      json_record_path: "results"
    pagination:
      mode: "cursor"
      cursor_param: "cursor"
      next_cursor_path: "meta.next"
      page_size_param: "limit"
      page_size_value: 1000
    backfill:
      enabled: true
      strategy: "cursor"
      cursor:
        start_value: null
        stop_when_older_than:
          field: "created_at"
          value: "2025-01-01T00:00:00Z"

  # --- Link-header pagination ---
  link_header_api:
    path: "v1/resources"
    parse: { type: "json", json_record_path: "data" }
    pagination: { mode: "link-header" }
    backfill: { enabled: false }

  # --- No pagination + link expansion ---
  orders_with_details:
    path: "v1/orders"
    parse: { type: "json", json_record_path: "orders" }
    pagination: { mode: "none" }
    link_expansion:
      enabled: true
      url_fields: ["detail_url", "links.detail"]
      json_record_path: null
    output:    # per-table override example
      format: jsonl
      s3:
        bucket: "my-ingest-bucket"
        prefix: "{env}/orders-with-details/{today:%Y/%m/%d}"
```

### Output behavior

- **CSV**: optional `compression: gzip` → `ContentEncoding=gzip`.
- **JSONL**: `ContentType=application/x-ndjson`.
- **Parquet**: `compression` (e.g., `snappy`).
- S3 key: `prefix/filename`, where default filename is `{table}-{now:%Y%m%dT%H%M%SZ}.{ext}`.
  - Templating tokens: `{table}`, `{env}`, `{now}`, `{today}`.

---

## Pagination modes (summary)

- `none` — single `GET`.
- `page` — increments `page_param` until an **empty** page is encountered (the empty page is **not** appended).
- `cursor` — either:
  - **opaque token** via `next_cursor_path` → send with `cursor_param`, or
  - **chain field** via `chain_field` (last row’s value becomes next cursor).
- `link-header` — follows RFC5988 `Link: <...>; rel="next"`.
- `salesforce` — stops when `done` is `true` or `nextRecordsUrl` is missing; follows **relative** `nextRecordsUrl`.

---

## Backfill strategies

- `date` — windows controlled by `window_days`, `start_param`, `end_param`, `date_format`.
- `soql_window` (Salesforce) — half‑open windows `[start, end)`; first call per window sends `?q=<SOQL>`, paginator follows `nextRecordsUrl`.
- `cursor` — supports:
  - `cursor.start_value`
  - `cursor.stop_at_item` `{field, value, inclusive}`
  - `cursor.stop_when_older_than` `{field, value}` (ISO 8601)

---

## Link expansion

- Set `link_expansion.enabled: true` and provide `url_fields` (dot paths).
- Session **headers/auth/proxies/verify** are inherited automatically.
- Expanded responses **do not inherit** the base `json_record_path` unless you set `link_expansion.json_record_path`. By default we parse the expanded payload **as-is**, which is better for object endpoints.

---

## Logging & redaction

Logs redact common sensitive elements:
- Headers: `authorization`, `x-api-key`, `api-key`, `proxy-authorization`
- Params: `access_token`, `token`, `apikey`, `api_key`, `authorization`, `signature`

---

## Testing

### Unit tests (pytest)

```bash
pytest -q
```

Covers:
- env var expansion
- pagination modes
- backfills (date, SOQL window, cursor)
- link expansion (session inheritance + parse override)
- redaction & request kwarg whitelisting

### Component tests (behave)

```bash
behave
```

Scenarios use `responses` to mock HTTP:
- cursor, page, link‑header
- link expansion with auth
- date backfill windows
- Salesforce SOQL windowing

---

## Troubleshooting

- **Missing global output**: The ingestor requires an S3 sink; define `apis.output` or a per‑table `output`.
- **Parquet writer missing**: Install `pyarrow` or `fastparquet`.
- **Auth not applied in link expansion**: Ensure headers/auth are in **request_defaults** or table‑level; they’re copied to the session in `_apply_session_defaults`.
- **Salesforce next URL**: We join `nextRecordsUrl` **relative** to the host root.

---

## Changelog (recent)

- **Session defaults applied** once to ensure link expansion inherits `headers/auth/proxies/verify`.
- **Page pagination** stops **before** appending an empty page.
- **Link expansion parse** no longer inherits base `json_record_path` by default; can be overridden via `link_expansion.json_record_path`.
- Added **mandatory output sink** (`apis.output`) with S3 metadata in return object.
- Cleaned up config merging: table‑level `path`, `headers`, `params`, `pagination` override/merge global settings.
