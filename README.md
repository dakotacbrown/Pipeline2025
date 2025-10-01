# Pipeline2025

# ApiIngestor

A tiny, config-driven data ingestor for REST APIs. Point it at a YAML config, and it handles:

- `requests.Session` with retries/backoff
- JSON or CSV parsing into `pandas.DataFrame`
- Pagination (`none`, `cursor`, `page`, `link-header`, Salesforce)
- Windowed backfills (`date`, `soql_window`, `cursor`)
- Optional per-row link expansion (follow URLs in each row)
- `${ENV_VAR}` substitution in config
- Safe logging with secret redaction

It’s designed so **all tables share the same base API path**, with table-specific settings (params/parse/backfill) layered on top.

---

## Install

```bash
pip install pandas requests urllib3 pytest
```

---

## Quick start

```python
import logging, yaml
from api_ingestor import ApiIngestor  # your module path

with open("config.yml") as f:
    cfg = yaml.safe_load(f)

log = logging.getLogger("ingestor")
log.setLevel(logging.INFO)
log.addHandler(logging.StreamHandler())

ing = ApiIngestor(cfg, log)

# One-off pull
df = ing.run_once(table_name="accounts", env_name="prod")

# Windowed backfill (inclusive dates for generic/date strategy)
from datetime import date
bf = ing.run_backfill(
    table_name="accounts",
    env_name="prod",
    start=date(2024,1,1),
    end=date(2024,1,31),
)
```

---

## Config file (YAML)

Top-level sections: `envs` and `apis`.

```yaml
envs:
  dev:
    base_url: "https://api.vendor-a.com/"
  prod:
    base_url: "https://your.example.com/"

apis:
  # ---- Global API defaults (shared across all tables) ----
  request_defaults:
    headers:
      Authorization: "Bearer ${API_TOKEN}"   # expanded at runtime
      Accept: "application/json"
    timeout: 60
    verify: true
    # You may also include: auth, proxies
  retries:                      # session-level retry/backoff
    total: 4
    backoff_factor: 0.5
    status_forcelist: [429, 500, 502, 503, 504]
    allowed_methods: ["GET"]

  path: "services/data/v61.0/query"  # shared path for *all* tables

  pagination:                        # default paginator (SF example)
    mode: "salesforce"
    done_path: "done"
    next_url_path: "nextRecordsUrl"
    clear_params_on_next: true
    max_pages: 20000

  link_expansion:                    # global defaults (optional)
    enabled: true
    url_fields: ["detail_url", "links.self"]
    json_record_path: null
    per_request_delay: 0.0
    timeout: 30

  # ---- Tables (each key below is a table_name) ----

  # Salesforce SOQL windowed backfill (uses the global path/pagination above)
  marketing_reports:
    parse:
      type: "json"
      json_record_path: "records"
      json_drop_keys_any_depth: ["attributes"]
    params: {}                       # initial params merged over request_defaults.params
    backfill:
      enabled: true
      strategy: "soql_window"
      date_field: "LastModifiedDate"     # e.g., or SystemModstamp
      date_format: "%Y-%m-%dT%H:%M:%SZ"
      window_days: 7
      per_request_delay: 0.0
      soql_template: |
        SELECT Id, Name, LastModifiedDate
        FROM Account
        WHERE {date_field} >= {start}
          AND {date_field} <  {end}
        ORDER BY {date_field} ASC

  # Generic date-parameter windowing (non-SF)
  invoices:
    parse:
      type: "json"
      json_record_path: "data.items"
    params:
      include: "payments"
    backfill:
      enabled: true
      strategy: "date"
      window_days: 3
      start_param: "start_date"
      end_param: "end_date"
      date_format: "%Y-%m-%d"
      per_request_delay: 0.2

  # Cursor-bounded backfill (no date windows)
  events_cursor:
    parse:
      type: "json"
      json_record_path: "data"
    params: {}
    backfill:
      enabled: true
      strategy: "cursor"
      cursor:
        start_value: "CURSOR_123"
        stop_when_older_than:
          field: "timestamp"
          value: "2024-01-01T00:00:00Z"
    # You can override global pagination if needed:
    # pagination:
    #   mode: "cursor"
    #   next_cursor_path: "meta.next"
    #   cursor_param: "cursor"
    #   page_size_param: "limit"
    #   page_size_value: 1000
    #   max_pages: 10000

  # CSV table (single page or link-header pagination)
  finance_exports:
    parse:
      type: "csv"
    params: {}
    # pagination:
    #   mode: "link-header"
```

### Notes on config

- **Environment variable expansion**: any string with `${NAME}` gets replaced with `os.environ["NAME"]`. If the env var is missing, the literal `${NAME}` remains (so you can detect it).
- **Retries** can live under `apis.retries` or inside `apis.request_defaults`; the class will pick them up either way.
- **Session defaults applied globally**: headers/auth/proxies/verify from `request_defaults` are applied once to the `requests.Session`, so **link expansion requests inherit them**.

---

## Pagination modes

- `none` – one GET
- `salesforce` – uses `done` and `nextRecordsUrl` (relative)  
  Options: `done_path`, `next_url_path`, `clear_params_on_next`, `max_pages`
- `cursor` – looks up `next_cursor_path` (e.g., `meta.next`) and sends it as `cursor_param`  
  Options: `next_cursor_path`, `cursor_param`, `page_size_param`, `page_size_value`, `max_pages`
- `page` – increments a numeric `page_param` until the page is empty  
  Options: `page_param`, `start_page`, `page_size_param`, `page_size_value`, `max_pages`
- `link-header` – follows `Link: <...>; rel="next"` header

---

## Backfill strategies

- `date` *(default)* – Injects two date params per window (e.g., `start_date`/`end_date`) and paginates within each window.
- `soql_window` – Salesforce: build SOQL per half-open window `[start, end)` and let SF pagination run within the slice.
- `cursor` – Start from a cursor (token or ID chaining) and stop by item or time cutoff.

---

## Link expansion

- Enabled with `link_expansion.enabled: true`.
- `url_fields`: list of dot-paths in the base rows that contain URLs to GET (e.g., `"detail_url"`, `"links.self"`).
- `json_record_path`: if expanded responses have a different record path than the base page, set it here (e.g., `"items"`).
- `timeout`: optional per-request timeout for expansions (defaults to 30s if omitted).
- Inherits **auth/headers/verify/proxies** from the session via `_apply_session_defaults`.

> **Tip:** If your expanded payload parses to an empty frame, check `link_expansion.json_record_path`.

---

## Logging & redaction

- `_log_request` prints URL, params, headers **with sensitive keys redacted**:
  - Headers: `authorization`, `x-api-key`, `api-key`, `proxy-authorization`
  - Params: `access_token`, `token`, `apikey`, `api_key`, `authorization`, `signature`
- `_log_exception` includes full stack traces.

---

## Error handling

- Missing `envs.<env>.base_url` → `ValueError`
- Unknown `apis.<table>` → `KeyError`
- Unsupported parse type/pagination mode → `ValueError`
- HTTP errors raise via `resp.raise_for_status()` and are logged.

---

## API reference

```python
run_once(table_name: str, env_name: str) -> pd.DataFrame
run_backfill(table_name: str, env_name: str, start: date, end: date) -> pd.DataFrame
```

---

## Example usage (Salesforce SOQL window)

```python
from datetime import date
df = ing.run_backfill(
    table_name="marketing_reports",
    env_name="prod",
    start=date(2024, 1, 1),
    end=date(2024, 1, 31),
)
```

---

## Testing (pytest)

### Run tests

```bash
pytest -q
```

### What the unit tests cover

- Environment variable expansion for headers/params
- Applying session defaults (headers/auth/proxies/verify)
- Pagination modes:
  - `none` (single page)
  - `salesforce` (`done` + `nextRecordsUrl`)
  - `cursor` (opaque token)
  - `page`
  - `link-header`
- Backfills:
  - Generic date windows (merging window params)
  - SOQL half-open windows
  - Cursor-bounded with stop conditions
- Link expansion:
  - Inherits session defaults
  - Respects `link_expansion.json_record_path`
  - Honors per-request expansion `timeout`
- Redaction in request logs
- Error paths (missing base_url, unknown table, unsupported parse type)

### Example test (link expansion record path)

```python
import os
import logging
import pandas as pd
import pytest

class FakeResponse:
    def __init__(self, json_data=None, text=None, headers=None, links=None):
        self._json = json_data
        self._text = text or ""
        self.status_code = 200
        self.headers = headers or {}
        self.links = links or {}
        self.content = (text or "").encode("utf-8")

    def json(self): return self._json
    def raise_for_status(self): pass

class RecordingSession:
    """A minimal fake requests.Session that records calls and supports defaults."""
    def __init__(self):
        self.headers = {}
        self.proxies = {}
        self.verify = True
        self.auth = None
        self.calls = []  # list of (url, kwargs)
        self._queue = []

    def mount(self, *_args, **_kwargs): pass
    def queue(self, *responses): self._queue.extend(responses)

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        # pop next queued response
        if not self._queue:
            raise AssertionError("No queued responses left")
        return self._queue.pop(0)

@pytest.fixture
def make_ingestor(monkeypatch):
    import api_ingestor  # your module name

    def _make(config, responses):
        # env var used in Authorization
        os.environ["TOKEN"] = "XYZ"

        # logger
        log = logging.getLogger("t")
        log.setLevel(logging.INFO)
        log.handlers[:] = [logging.StreamHandler()]

        ing = api_ingestor.ApiIngestor(config, log)

        sess = RecordingSession()

        # monkeypatch _build_session to return our RecordingSession
        monkeypatch.setattr(api_ingestor, "requests", api_ingestor.requests)
        monkeypatch.setattr(api_ingestor, "Session", RecordingSession)
        monkeypatch.setattr(ing, "_build_session", lambda _r=None: sess)

        # queue responses
        sess.queue(*responses)
        return ing, sess
    return _make

def test_link_expansion_inherits_session_defaults_and_timeout(make_ingestor):
    import api_ingestor  # your module name

    config = {
        "envs": {"prod": {"base_url": "http://api/" }},
        "apis": {
            "request_defaults": {
                "headers": {"Authorization": "Bearer ${TOKEN}"},
                "verify": False
            },
            "path": "base",
            "pagination": {"mode": "none"},
            "link_expansion": {
                "enabled": True,
                "url_fields": ["detail_url"],
                "timeout": 5,
                "json_record_path": "items"
            },
            "t": {"parse": {"type": "json", "json_record_path": "data"}},
        },
    }

    base = FakeResponse(json_data={"data": [{"detail_url": "http://exp/detail"}]})
    expanded = FakeResponse(json_data={"items": [{"k": 1}]})
    responses = [base, expanded]

    ing, sess = make_ingestor(config, responses)

    df = ing.run_once("t", "prod")
    assert "k" in df.columns
    assert df["k"].tolist() == [1]
    assert len(sess.calls) == 2

    # 2nd call is the expansion GET → only per-request timeout should be present
    exp_kwargs = sess.calls[1][1]
    assert "timeout" in exp_kwargs and len(exp_kwargs) == 1

    # Session defaults were applied globally (so expansion inherits them)
    assert sess.headers.get("Authorization") == "Bearer XYZ"
    assert sess.verify is False
```

> You can follow the same pattern to test other modes: queue multiple `FakeResponse`s to simulate pages and verify the resulting DataFrame and `sess.calls`.

---

## Design details

- **“Fix #2”**: `_apply_session_defaults` copies request defaults (headers/auth/proxies/verify) to the `Session` once. This guarantees **every** request—especially link expansion calls—carries the same auth/headers/TLS/proxy settings without re-passing kwargs on each call.
- **Env expansion** leaves unresolved `${NAME}` literals intact rather than silently replacing with empty strings.
- **Normalization**: `_json_obj_to_df` respects `json_record_path`. If it points to a list → `DataFrame(list)`, else `json_normalize(dict)`.

---

## Troubleshooting

- **Empty DataFrame on link expansion**: set `apis.link_expansion.json_record_path` (or override per table) to match the expanded payload (e.g., `"items"`).
- **Next page never reached**: check `pagination.next_cursor_path` or `page_param/start_page` values.
- **Salesforce stops at first page**: make sure `pagination.done_path` & `next_url_path` match the response, and `clear_params_on_next: true` if you use an initial `q` param.
