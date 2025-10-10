# API Ingestor

A modular, testable framework for pulling data from HTTP APIs into tabular form. It provides:

- A **unified paginator** with multiple modes (`none`, `cursor`, `page`, `link-header`, `salesforce`)
- **Parsing** into pandas DataFrames (JSON-first, with helpers for nested objects)
- **Backfill** strategies (date windows, cursor-driven, Salesforce SOQL windows)
- **Link expansion** (N+1 “follow-up” requests per row, with session-aware flushing)
- **Output** to S3 (CSV/Parquet/JSONL), including partial “flush” modes
- **Retries**, request defaults, and logging hooks
- **Composable multi-pulls** with joins

It’s designed for reliability in production **and** easy mocking in tests.

---

## Quickstart

```python
from api_ingestor import ApiIngestor
import yaml

with open("config.yaml") as f:
    config = yaml.safe_load(f)

ingestor = ApiIngestor(config=config, log=your_logger)

# One-shot pull (uses apis.<table> config)
meta = ingestor.run_once(table_name="my_table", env_name="dev")
print(meta["rows"], meta["s3_uri"])

# Backfill (date-window, cursor, or SOQL-window based on apis.<table>.backfill)
meta = ingestor.run_backfill(
    table_name="my_table",
    env_name="dev",
    start=date(2024, 1, 1),
    end=date(2024, 1, 31),
)
print(meta["rows"], meta["strategy"], meta["s3_uri"])
```

---

## The orchestrator: `ApiIngestor`

File: `api_ingestor.py`

- `run_once(table_name, env_name)`:
  1. Loads config via `config.prepare(...)`
  2. Builds a `requests.Session` with retries (`request_helpers.build_session`)
  3. Applies session defaults (headers, timeout, TLS verify)
  4. Builds a context (logger, output hooks, link-expansion session tracking)
  5. If `multi_pulls` is configured → `run_multi_pulls_with_join(...)`
     else → `paginate(...)`
  6. Optional link expansion on the resulting DataFrame
  7. Writes the final DataFrame via `output.write_output(...)`
  8. Returns metadata (rows, format, S3 URI, timings, pagination mode)

- `run_backfill(table_name, env_name, start, end)`:
  Chooses one of:
  - **`strategy = "cursor"`** → `backfill.cursor_backfill(...)`
  - **`strategy = "soql_window"`** → `backfill.soql_window_backfill(...)`  
    (requires `pagination.mode == "salesforce"`)
  - **default “date-window”**: Slides windows of `window_days`, injects
    `start_param`/`end_param`, and calls `paginate(...)` (or `multi_pulls`), concatenating results.

Both methods share the same context and link-expansion semantics, so behavior is consistent.

---

## Pagination (`pagination.py`)

### Modes

- `none`  
  One GET → `parsing.to_dataframe(...)` → return.

- `cursor`  
  Injects optional `page_size_param/page_size_value`.  
  On each page:
  - `to_dataframe(...)`
  - `next_cursor = dig(resp.json(), next_cursor_path)`
  - If present: set `params[cursor_param] = next_cursor` and **re-call the same URL**
  - Stop when `max_pages` reached or the API stops returning a cursor

- `page`  
  Injects page size and `page_param` (starting at `start_page`).  
  Stops on first empty page **or** `max_pages` (whichever first).

- `link-header`  
  Follow `resp.links["next"]["url"]` until missing (or `max_pages`).

- `salesforce`  
  Special handling for SOQL:
  - The **first** URL is composed as `f"{base_url}?q={encoded_soql}"`.
  - `q` is **removed from** `safe["params"]` so `requests` doesn’t form-encode it.
  - SOQL encoding is RFC 3986 query-component style, with:
    - spaces → `%20` (never `+`)
    - commas stay as `,`
    - operators/quotes percent-encoded
    - lowercase `z` → `Z` in timestamps, to normalize ISO forms
  - After first page:
    - It reads `nextRecordsUrl` and joins via `urljoin(host_base, nxt)`
    - If `clear_params_on_next` is true, **all params are dropped** for follow-up calls
  - Records are parsed by `parsing.json_obj_to_df(...)`, and keys listed in `json_drop_keys_any_depth` are removed before parsing (common for Salesforce’s `"attributes"` objects).

Some stacks (e.g., AWS Glue 4.0/5.0 + parts of `requests`) will form-encode query params (`+` for spaces), which **Salesforce SOQL endpoints don’t like**. We explicitly percent-encode the `q` in the **URL**, not `params`, to ensure:
- spaces are `%20`, not `+`
- commas remain `,`
- reserved characters are encoded

This mirrors what tools like Insomnia’s URL preview show and removes ambiguity.

---

## Backfill (`backfill.py`)

Two specialized strategies plus the default date-window flow:

### 1) Cursor backfill: `cursor_backfill(...)`

- Accepts:
  - `pag_cfg` (cursor pagination config):  
    `next_cursor_path`, `cursor_param`, `page_size_param/value`, `max_pages`
  - `cur_cfg` (backfill-specific guards):  
    - `start_value`: seed cursor token (placed into `params[cursor_param]` **before the first call**)
    - `stop_at_item`: `{ field, value, inclusive }` → trim the page when that item is seen and **stop**
    - `stop_when_older_than`: `{ field, value }` → keep only rows with timestamp ≥ value and **stop**

- Loop:
  1. GET the current URL with `safe` request options
  2. Parse to a page DataFrame via `to_dataframe(...)`
  3. Apply **stop-by-item** or **stop-by-time** if configured; trim and stop as needed
  4. Append the page if non-empty
  5. Find `next_cursor` (`dig(json, next_cursor_path)`), and if present, set `params[cursor_param] = token` and continue; otherwise stop
  6. Respect `max_pages`

- **Expansion**: by design, link expansion is **single-shot** on the aggregated result (after the loop), so you don’t balloon work by expanding each page separately. This mirrors `run_once` behavior.

> If your API **requires** the very first call to include a seed cursor (e.g., `"cursor" = "abc123"`), put that value under `backfill.cursor.start_value` in your config. The code injects it before the first request.

### 2) Salesforce SOQL window backfill: `soql_window_backfill(...)`

- Requires `pagination.mode == "salesforce"`
- Slides windows of `window_days`, building a SOQL from a `soql_template`:
  ```
  SELECT Id, Name
  FROM Account
  WHERE {date_field} >= {start} AND {date_field} < {end}
  ORDER BY {date_field} ASC
  ```
- For each window:
  - Inject `q` = SOQL into **params** (Salesforce mode later moves it to URL & encodes)
  - `paginate(...)` to follow `nextRecordsUrl`
  - Optional link expansion
  - Optional per-request delay (`per_request_delay`)
- Concatenate windows.

### 3) Default date-window backfill (non-SF)

- Slides windows by `window_days`
- Injects `start_param`/`end_param` with `date_format`
- Calls `paginate(...)` or `run_multi_pulls_with_join(...)`
- Optional link expansion and per-request delay
- Concatenate windows

---

## Parsing (`parsing.py`)

- **`to_dataframe(resp, parse_cfg)`** delegates to format-specific handlers; common path is JSON:
  - `json_record_path` (dotted path) selects the array to expand into rows
  - If missing, it tries to coerce the top-level JSON sensibly
- **`json_obj_to_df(obj, parse_cfg)`** is used by Salesforce pagination: it reads the `records` array and applies drops
- **`drop_keys_any_depth(obj, keys)`** removes noisy metadata (e.g., SF `"attributes"`) anywhere in the payload

The result is a `pandas.DataFrame`. Empty arrays produce an empty DataFrame (shape `(0, 0)` or `(0, n)` depending on normalization).

---

## Link expansion (`link_expansion.py`)

Use this to “follow” URLs found in each row and merge/flush the results.

- **Configuration**:
  - `url_fields`: a list of columns (or nested fields via dotted paths) that contain URLs
  - Optional parser override (`type`, `json_record_path`) specifically for expansions
  - `per_request_delay`: throttle requests
  - Session-id extraction (from the URL) and **policy**:
    - `regex`, `group` → pull a session token
    - `policy`: `"first" | "last" | "all" | "require_single"`
  - **Flush** modes:
    - `none`: build a single expanded DataFrame
    - `per_link`: flush each expansion to S3 as it’s fetched
    - `per_session`: gather expansions by session id, flush one artifact per session

- **How it works**:
  1. Iterate each input row
  2. Collect all URLs across `url_fields`
  3. GET each URL, parse into a small DataFrame
  4. **Merge parts for the row** on the index (outer join) so multiple URLs can enrich the row
  5. Depending on `flush.mode`, either accumulate or push partials to S3
  6. Concatenate expanded frames at the end

- **Session id in outputs**:
  When flushing, the code adds `session_id` and `seq` to `ctx["current_output_ctx"]` so `output.write_s3(...)` can incorporate them into file paths/names if your S3 writer uses those.

---

## Multipull (`multipull.py`)

Multipull composes multiple API calls and joins them (e.g., main list + enrichments keyed by id). You define the pulls and their join keys in config; `run_multi_pulls_with_join(...)`:
- Executes each configured sub-request (reusing the same session/defaults)
- Parses each into a DataFrame
- Joins (merge) into a single table

It’s useful when link expansion isn’t the right shape (e.g., the “join” is many-to-one, or you’re hitting distinct list endpoints).

---

## Output (`output.py`)

- **`write_output(ctx, df, output_cfg)`** is the top-level writer used by `ApiIngestor`.
  - **Requires** an output config (`apis.<table>.output` or `apis.output`). If missing, it raises:
    ```
    ValueError: Output config is required but missing (apis.<table>.output or apis.output).
    ```
  - Honors `format` (`csv` | `parquet` | `jsonl`), `write_empty`, and `s3` settings
  - Returns metadata dict:
    ```python
    {
      "format": "csv",
      "s3_bucket": "...",
      "s3_key": "...",
      "s3_uri": "s3://.../...",
      "bytes": 12345,
    }
    ```

- **`write_s3(ctx, table, env, df, fmt, s3_cfg)`** is injected into the context and used by both final writes and link-expansion flushes. It’s your S3 integration point (naming, compression, region, etc.). In tests we often stub this.

**Flushes** (from link expansion) pass `session_id` and `seq` in `ctx["current_output_ctx"]` so your writer can emit deterministic part files.

---

## Request helpers (`request_helpers.py`)

- `build_session(retries_cfg)`  
  Returns a `requests.Session` with `urllib3`-style retry/backoff based on config:
  - `total`, `backoff_factor`, `status_forcelist`, `allowed_methods`
- `apply_session_defaults(session, safe_defaults)`  
  Applies headers, timeout, and TLS settings
- `build_url(base_url, path)`  
  Joins environment `base_url` with per-table `path`
- `log_request(ctx, url, request_opts, prefix="")` and `log_exception(...)`  
  Hook points used throughout `ApiIngestor` and backfills

---

## Small utils (`small_utils.py`)

- `whitelist_request_opts(opts)`  
  Filter request kwargs to a safe subset for `requests.get(...)` (e.g., `params`, `headers`, `timeout`, `verify`, `auth`, `json`, `data`)
- `dig(obj, dotted_path)`  
  Safe traversal of nested dicts/lists using a dotted path (used by cursor pagination to find `next_cursor_path`)
- `get_from_row(row, dotted_path)`  
  Pulls nested fields from a pandas row for link expansion

---

## End-to-end control flow

### `run_once` (no multipull)

```
prepare() → build_session() → apply_session_defaults()
→ log_request() → paginate()
→ (optional) expand_links()
→ write_output()
```

### `run_backfill(strategy="cursor")`

```
prepare() → build_session() → apply_session_defaults()
→ cursor_backfill()
   ↳ seed params[cursor_param] = start_value (if provided)
   ↳ loop pages via next_cursor_path, max_pages guard
   ↳ single-shot expand_links() on the final concatenated result
→ write_output()
```

### `run_backfill(strategy="soql_window")`

```
window over dates:
  format SOQL (soql_template + {start}..{end})
  req_opts["params"]["q"] = soql
  → paginate(salesforce-mode) (encodes q into URL; removes from params)
  → (optional) expand_links()
  → (optional) sleep between windows
concat windows → write_output()
```

### `run_backfill(strategy="date")`

```
for each window:
  inject start_param/end_param
  → paginate() or run_multi_pulls_with_join()
  → (optional) expand_links()
  → (optional) per-window sleep
concat → write_output()
```

---

## Testing notes

- **Unit tests**: use simple fakes for `Session`/`Response` and patch `to_dataframe`/`expand_links` to isolate control flow.
