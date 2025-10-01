import json
import textwrap
from datetime import datetime
from urllib.parse import parse_qs, urljoin, urlparse

import pandas as pd
import responses
from behave import given, then, when

# Try to import the ApiIngestor from user's repo (project root must be on sys.path)
from utils.api_ingestor import ApiIngestor


@given('a base API host "{base}"')
def step_base_host(context, base):
    context.base = base.rstrip("/") + "/"
    # Minimal config envs
    context.ingestor_config = {
        "envs": {"dev": {"base_url": context.base}},
        "apis": {
            # global defaults; table-specific blocks added by steps
            "request_defaults": {"headers": {}, "timeout": 10, "verify": True},
            "path": "",  # we'll override per-table or use full URLs in mocks
        },
    }

    # Simple logger stub
    class _Logger:
        def info(self, *args, **kwargs):
            pass

        def error(self, *args, **kwargs):
            pass

    context.logger = _Logger()


# ----------------- Cursor pagination -----------------


@given('a cursor endpoint "{path}" with two pages of 2 items each')
def step_cursor_endpoint(context, path):
    full1 = urljoin(context.base, path.lstrip("/"))
    full2 = urljoin(
        context.base, path.lstrip("/")
    )  # same path; next token advances via params
    # Page 1
    context.responses.add(
        responses.GET,
        full1,
        json={"items": [{"id": 1}, {"id": 2}], "meta": {"next": "t2"}},
        status=200,
        content_type="application/json",
        match=[responses.matchers.query_param_matcher({})],
    )
    # Page 2 (requires cursor=t2)
    context.responses.add(
        responses.GET,
        full2,
        json={"items": [{"id": 3}, {"id": 4}], "meta": {}},
        status=200,
        content_type="application/json",
        match=[responses.matchers.query_param_matcher({"cursor": "t2"})],
    )


@given("a cursor-table config")
def step_cursor_config(context):
    context.ingestor_config["apis"]["path"] = "cursor"  # appended to base
    context.ingestor_config["apis"]["pagination"] = {
        "mode": "cursor",
        "next_cursor_path": "meta.next",
        "cursor_param": "cursor",
        "page_size_param": None,
        "page_size_value": None,
        "max_pages": 10,
    }
    context.ingestor_config["apis"]["cursor_table"] = {
        "parse": {"type": "json", "json_record_path": "items"}
    }


# ----------------- Page pagination -----------------


@given('a paged endpoint "{path}" where page 1 has 2 items and page 2 is empty')
def step_paged_endpoint(context, path):
    full = urljoin(context.base, path.lstrip("/"))
    # Page 1
    context.responses.add(
        responses.GET,
        full,
        json=[{"id": "A"}, {"id": "B"}],
        status=200,
        content_type="application/json",
        match=[responses.matchers.query_param_matcher({"page": "1"})],
    )
    # Page 2 -> empty
    context.responses.add(
        responses.GET,
        full,
        json=[],
        status=200,
        content_type="application/json",
        match=[responses.matchers.query_param_matcher({"page": "2"})],
    )


@given("a page-table config")
def step_page_config(context):
    context.ingestor_config["apis"]["path"] = "paged"
    context.ingestor_config["apis"]["pagination"] = {
        "mode": "page",
        "page_param": "page",
        "start_page": 1,
        "max_pages": 10,
    }
    context.ingestor_config["apis"]["page_table"] = {"parse": {"type": "json"}}


# ----------------- Link-header pagination -----------------


@given('a link-header chain starting at "{first}" then "{second}"')
def step_link_header_chain(context, first, second):
    url1 = urljoin(context.base, first.lstrip("/"))
    url2 = urljoin(context.base, second.lstrip("/"))
    # First page with Link header to next
    context.responses.add(
        responses.GET,
        url1,
        json=[{"id": 1}],
        headers={"Link": f'<{url2}>; rel="next"'},
        status=200,
        content_type="application/json",
    )
    # Second page without next
    context.responses.add(
        responses.GET,
        url2,
        json=[{"id": 2}],
        status=200,
        content_type="application/json",
    )


@given("a link-header-table config")
def step_link_header_config(context):
    context.ingestor_config["apis"]["path"] = "link1"
    context.ingestor_config["apis"]["pagination"] = {
        "mode": "link-header",
        "max_pages": 10,
    }
    context.ingestor_config["apis"]["link_header_table"] = {
        "parse": {"type": "json"}
    }


# ----------------- Link expansion -----------------


@given('a base endpoint "{path}" that returns items with per-row detail URLs')
def step_rows_with_details(context, path):
    base_url = urljoin(context.base, path.lstrip("/"))
    # rows return two items that each point to detail endpoints
    payload = {
        "items": [
            {"id": 1, "detail_url": urljoin(context.base, "detail/1")},
            {"id": 2, "detail_url": urljoin(context.base, "detail/2")},
        ]
    }
    context.responses.add(
        responses.GET,
        base_url,
        json=payload,
        status=200,
        content_type="application/json",
    )


@given("detail endpoints require Authorization header")
def step_detail_requires_auth(context):
    # Define callbacks that verify the Authorization header, else 401
    def _cb(request):
        if request.headers.get("Authorization") == "Bearer TESTTOKEN":
            return (
                200,
                {"Content-Type": "application/json"},
                json.dumps({"value": "ok1"}),
            )
        return (401, {}, "")

    def _cb2(request):
        if request.headers.get("Authorization") == "Bearer TESTTOKEN":
            return (
                200,
                {"Content-Type": "application/json"},
                json.dumps({"value": "ok2"}),
            )
        return (401, {}, "")

    context.responses.add_callback(
        responses.GET,
        urljoin(context.base, "detail/1"),
        callback=lambda request: _cb(request),
    )
    context.responses.add_callback(
        responses.GET,
        urljoin(context.base, "detail/2"),
        callback=lambda request: _cb2(request),
    )


@given('a link-expansion-table config with Authorization "{token}"')
def step_link_expansion_config(context, token):
    context.ingestor_config["apis"]["request_defaults"] = {
        "headers": {"Authorization": token, "Accept": "application/json"},
        "timeout": 30,
        "verify": True,
    }
    context.ingestor_config["apis"]["path"] = "rows"
    context.ingestor_config["apis"]["pagination"] = {"mode": "none"}

    context.ingestor_config["apis"]["link_exp_table"] = {
        "parse": {
            "type": "json",
            "json_record_path": "items",  # base response is {"items":[...]}, keep this
        },
        "link_expansion": {
            "enabled": True,
            "url_fields": ["detail_url"],
            # IMPORTANT: don't set json_record_path here since detail is a single object
            # "json_record_path": null
        },
    }


# ----------------- Date-window backfill -----------------


@given('a date-window endpoint "{path}" that echoes window ranges as items')
def step_date_window_endpoint(context, path):
    full = urljoin(context.base, path.lstrip("/"))

    def callback(request):
        # Extract params and echo days as items
        parsed = urlparse(request.url)
        q = parse_qs(parsed.query)
        start = q.get("start_date", [""])[0]
        end = q.get("end_date", [""])[0]
        # produce one item per day inclusively
        d0 = pd.to_datetime(start).date()
        d1 = pd.to_datetime(end).date()
        days = [
            (d0 + pd.Timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range((d1 - d0).days + 1)
        ]
        return (
            200,
            {"Content-Type": "application/json"},
            json.dumps([{"day": d} for d in days]),
        )

    context.responses.add_callback(responses.GET, full, callback=callback)


@given("a date-backfill-table config with 2-day windows")
def step_date_backfill_config(context):
    context.ingestor_config["apis"]["path"] = "events"
    context.ingestor_config["apis"]["pagination"] = {"mode": "none"}
    context.ingestor_config["apis"]["date_bf_table"] = {
        "parse": {"type": "json"},
        "backfill": {
            "enabled": True,
            "strategy": "date",
            "window_days": 2,
            "start_param": "start_date",
            "end_param": "end_date",
            "date_format": "%Y-%m-%d",
        },
    }


# ----------------- Salesforce SOQL-window backfill -----------------


@given('a salesforce endpoint "{path}" that returns done=false then done=true')
def step_salesforce_endpoint(context, path):
    base = urljoin(context.base, path.lstrip("/"))
    next_full = urljoin(context.base, "sf/query/next1")

    # first response: done=false, provide nextRecordsUrl
    context.responses.add(
        responses.GET,
        base,
        json={
            "done": False,
            "nextRecordsUrl": "/sf/query/next1",
            "records": [{"id": "a"}],
        },
        status=200,
        content_type="application/json",
    )
    # second response (follow nextRecordsUrl): done=true, no next
    context.responses.add(
        responses.GET,
        next_full,
        json={
            "done": True,
            "records": [{"id": "b"}],
        },
        status=200,
        content_type="application/json",
    )


@given("a soql-window-table config")
def step_sf_config(context):
    context.ingestor_config["apis"]["path"] = "sf/query"
    context.ingestor_config["apis"]["pagination"] = {
        "mode": "salesforce",
        "done_path": "done",
        "next_url_path": "nextRecordsUrl",
        "clear_params_on_next": True,
        "max_pages": 10,
    }
    soql = textwrap.dedent(
        """
        SELECT Id, Name, LastModifiedDate
        FROM Account
        WHERE {date_field} >= {start}
          AND {date_field} <  {end}
        ORDER BY {date_field} ASC
    """
    ).strip()
    context.ingestor_config["apis"]["sf_soql_table"] = {
        "parse": {
            "type": "json",
            "json_record_path": "records",
            "json_drop_keys_any_depth": ["attributes"],
        },
        "backfill": {
            "enabled": True,
            "strategy": "soql_window",
            "date_field": "LastModifiedDate",
            "date_format": "%Y-%m-%dT%H:%M:%SZ",
            "window_days": 1,
            "soql_template": soql,
        },
    }


# ----------------- When steps -----------------


@when('I run the ingestor once for table "{table}" in env "{env}"')
def step_run_once(context, table, env):
    ingestor = ApiIngestor(config=context.ingestor_config, log=context.logger)
    context.df = ingestor.run_once(table, env)


@when(
    'I run the backfill for table "{table}" in env "{env}" from "{start}" to "{end}"'
)
def step_run_backfill(context, table, env, start, end):
    ingestor = ApiIngestor(config=context.ingestor_config, log=context.logger)
    d0 = datetime.strptime(start, "%Y-%m-%d").date()
    d1 = datetime.strptime(end, "%Y-%m-%d").date()
    context.df = ingestor.run_backfill(table, env, d0, d1)


# ----------------- Then steps -----------------


@then("the result has {n:d} rows")
def step_assert_rows(context, n):
    assert hasattr(context, "df"), "No DataFrame on context"
    assert (
        len(context.df) == n
    ), f"Expected {n} rows, got {len(context.df)}.\n{context.df}"


@then(
    'the expanded result has {n:d} rows and includes the detail field "{col}"'
)
def step_assert_expanded(context, n, col):
    assert hasattr(context, "df"), "No DataFrame on context"
    assert (
        len(context.df) == n
    ), f"Expected {n} rows, got {len(context.df)}.\n{context.df}"
    assert (
        col in context.df.columns
    ), f"Expected column '{col}' in expanded DataFrame. Columns: {context.df.columns}"


@then('the result has {n:d} rows and contains values "{d1}" "{d2}" "{d3}"')
def step_assert_contains_days(context, n, d1, d2, d3):
    assert len(context.df) == n
    vals = set(context.df["day"].tolist())
    for d in (d1, d2, d3):
        assert d in vals, f"Missing {d} in {vals}"


@then('the result has {n:d} rows with ids "{a}" and "{b}"')
def step_assert_sf_rows(context, n, a, b):
    assert len(context.df) == n, context.df
    ids = set(context.df["id"].astype(str).tolist())
    assert a in ids and b in ids, ids
