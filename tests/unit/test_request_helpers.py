# tests/unit/test_http_extended.py
from requests import Session

from api_ingestor import request_helpers


class CaptureLog:
    def __init__(self):
        self.infos = []
        self.errors = []

    def info(self, msg):
        self.infos.append(msg)

    def error(self, msg):
        self.errors.append(msg)


def test_build_url_various_slashes():
    # base with trailing, path with leading
    assert request_helpers.build_url("https://api/", "/v1") == "https://api/v1"
    # base without trailing, path no leading
    assert request_helpers.build_url("https://api", "v1") == "https://api/v1"
    # both present, ensure no double slashes
    assert (
        request_helpers.build_url("https://api/", "v1/items")
        == "https://api/v1/items"
    )
    assert (
        request_helpers.build_url("https://api", "/v1/items")
        == "https://api/v1/items"
    )


def test_build_session_no_retries_returns_session():
    s = request_helpers.build_session(None)
    assert isinstance(s, Session)
    # default mounts still exist (requests has some defaults), but our function
    # adds explicit mounts only when retries_cfg is provided, so we just check type here.


def test_build_session_with_retries_normal_path():
    cfg = {
        "total": 2,
        "backoff_factor": 0.1,
        "status_forcelist": [500],
        "allowed_methods": ["GET", "POST"],
    }
    s = request_helpers.build_session(cfg)
    assert isinstance(s, Session)
    # verify our adapters were mounted (keys are exactly 'https://' and 'http://')
    assert "https://" in s.adapters
    assert "http://" in s.adapters


def test_build_session_fallback_to_method_whitelist(monkeypatch):
    """Force the code path where constructing Retry with `allowed_methods=...` raises
    TypeError, then ensure we fall back to `method_whitelist=...`."""
    calls = {"phase": []}

    class RetryPhase1:
        # first attempt: signature doesn't accept allowed_methods -> raise TypeError
        def __init__(self, *args, **kwargs):
            calls["phase"].append("phase1")
            if "allowed_methods" in kwargs:
                raise TypeError("no allowed_methods here")
            # Simulate Retry object attributes required later
            self.respect_retry_after_header = True

    class RetryPhase2:
        # second attempt: accepts method_whitelist
        def __init__(self, *args, **kwargs):
            calls["phase"].append("phase2")
            assert "method_whitelist" in kwargs
            self.respect_retry_after_header = True

    # Monkeypatch http.Retry in two steps: first call uses phase1, then when caught,
    # we swap it to phase2 for the inner try-block.
    # Simpler approach: patch http.Retry to a dispatcher that switches classes based on the kw.
    def RetryDispatcher(*args, **kwargs):
        if "allowed_methods" in kwargs:
            return RetryPhase1(*args, **kwargs)
        return RetryPhase2(*args, **kwargs)

    monkeypatch.setattr(request_helpers, "Retry", RetryDispatcher)

    cfg = {"total": 1, "allowed_methods": ["GET"]}
    s = request_helpers.build_session(cfg)
    assert isinstance(s, Session)
    assert "phase1" in calls["phase"]
    assert "phase2" in calls["phase"]
    # adapters mounted
    assert "https://" in s.adapters and "http://" in s.adapters


def test_apply_session_defaults_sets_fields():
    s = Session()
    request_helpers.apply_session_defaults(
        s,
        {
            "headers": {"X-Api-Key": "secret"},
            "auth": ("u", "p"),
            "proxies": {"https": "http://proxy"},
            "verify": False,
        },
    )
    # headers merged
    assert s.headers["X-Api-Key"] == "secret"
    # auth assigned
    assert s.auth == ("u", "p")
    # proxies merged
    assert s.proxies.get("https") == "http://proxy"
    # verify flag set
    assert s.verify is False


def test_log_request_redacts_sensitive_items():
    log = CaptureLog()
    ctx = {"log": log}
    request_helpers.log_request(
        ctx,
        "https://api/x",
        {
            "headers": {"Authorization": "Bearer abc", "X-Ok": "1"},
            "params": {"token": "zzz", "q": "hello"},
        },
        prefix="[p] ",
    )
    assert log.infos, "expected an info log line"
    line = log.infos[-1]
    # sensitive redacted
    assert "***REDACTED***" in line
    # non-sensitive preserved
    assert "X-Ok" in line and "1" in line
    assert "q" in line and "hello" in line
    # prefix present
    assert line.startswith("[p] GET https://api/x")


def test_log_exception_includes_url_and_stacktrace():
    log = CaptureLog()
    ctx = {"log": log}
    try:
        raise RuntimeError("boom")
    except Exception as e:
        request_helpers.log_exception(ctx, "https://api/x", e, prefix="[E] ")
    assert log.errors, "expected an error log line"
    err = log.errors[-1]
    assert "https://api/x" in err
    assert "Stack Trace:" in err
    assert err.startswith("[E] ")
