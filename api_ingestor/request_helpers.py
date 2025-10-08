import traceback
from typing import Any, Dict, Optional
from urllib.parse import urljoin

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_SENSITIVE_HEADERS = {
    "authorization",
    "x-api-key",
    "api-key",
    "proxy-authorization",
}
_SENSITIVE_PARAMS = {
    "access_token",
    "token",
    "apikey",
    "api_key",
    "authorization",
    "signature",
    "client_id",
    "client_secret",
    "refresh_token",
    "secret",
    "password",
    "private_key",
    "x-authorization",
    "auth",
}


def build_url(base_url: str, path: str) -> str:
    return urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))


def build_session(retries_cfg: Optional[Dict[str, Any]]) -> Session:
    s = Session()
    if not retries_cfg:
        return s
    total = int(retries_cfg.get("total", 3))
    connect = int(retries_cfg.get("connect", total))
    read = int(retries_cfg.get("read", total))
    backoff_factor = float(retries_cfg.get("backoff_factor", 0.5))
    status_forcelist = tuple(
        retries_cfg.get("status_forcelist", [429, 500, 502, 503, 504])
    )
    allowed = retries_cfg.get("allowed_methods", ["GET"])
    allowed_set = frozenset(m.upper() for m in allowed)
    base_kwargs = dict(
        total=total,
        connect=connect,
        read=read,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    try:
        r = Retry(
            **base_kwargs, allowed_methods=allowed_set, raise_on_status=False
        )
    except TypeError:
        try:
            r = Retry(
                **base_kwargs,
                method_whitelist=allowed_set,
                raise_on_status=False,
            )
        except TypeError:
            r = Retry(**base_kwargs, method_whitelist=allowed_set)

    adapter = HTTPAdapter(max_retries=r)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def apply_session_defaults(sess: Session, opts: Dict[str, Any]) -> None:
    if opts.get("headers"):
        sess.headers.update(opts["headers"])
    if "auth" in opts:
        sess.auth = opts["auth"]
    if opts.get("proxies"):
        sess.proxies.update(opts["proxies"])
    if "verify" in opts:
        sess.verify = opts["verify"]


def log_request(
    ctx: Dict[str, Any], url: str, opts: Dict[str, Any], prefix: str = ""
):
    log = ctx["log"]
    safe_headers = dict(opts.get("headers") or {})
    for k in list(safe_headers):
        if k.lower() in _SENSITIVE_HEADERS:
            safe_headers[k] = "***REDACTED***"
    safe_params = dict(opts.get("params") or {})
    for k in list(safe_params):
        if k.lower() in _SENSITIVE_PARAMS:
            safe_params[k] = "***REDACTED***"
    log.info(f"{prefix}GET {url} params={safe_params} headers={safe_headers}")


def log_exception(
    ctx: Dict[str, Any], url: str, e: Exception, prefix: str = ""
):
    ctx["log"].error(
        f"{prefix}Error retrieving data from {url}: {e}\nStack Trace: {traceback.format_exc()}"
    )
