import os
import re
from typing import Any, Dict

_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def expand_env_value(v: Any) -> Any:
    if isinstance(v, str):
        return _ENV_RE.sub(lambda m: os.getenv(m.group(1), m.group(0)), v)
    if isinstance(v, dict):
        return {k: expand_env_value(vv) for k, vv in v.items()}
    if isinstance(v, list):
        return [expand_env_value(x) for x in v]
    return v


def prepare(config: Dict[str, Any], table_name: str, env_name: str):
    """Return (env_cfg, api_cfg, req_opts, parse_cfg)."""
    env_cfg = (config.get("envs") or {}).get(env_name) or {}
    if not env_cfg.get("base_url"):
        raise ValueError(f"env '{env_name}' must define a non-empty base_url")

    apis_root = config.get("apis") or {}
    table_cfg = apis_root.get(table_name) or {}
    if not table_cfg:
        raise KeyError(f"Table config '{table_name}' not found under 'apis'.")

    # request defaults
    global_opts = apis_root.get("request_defaults", {}) or {}
    req_opts = dict(global_opts)
    for k in ("headers", "params"):
        tv = table_cfg.get(k)
        if isinstance(tv, dict):
            req_opts[k] = {**(req_opts.get(k) or {}), **tv}
    for k in ("timeout", "verify", "auth", "proxies"):
        if k in table_cfg:
            req_opts[k] = table_cfg[k]

    retries = (
        table_cfg.get("retries")
        or req_opts.get("retries")
        or apis_root.get("retries")
    )
    if retries:
        req_opts["retries"] = retries

    api_eff = {
        "path": table_cfg.get("path", apis_root.get("path", "")),
        "pagination": {
            **(apis_root.get("pagination", {}) or {}),
            **(table_cfg.get("pagination", {}) or {}),
        },
        "link_expansion": {
            **(apis_root.get("link_expansion", {}) or {}),
            **(table_cfg.get("link_expansion", {}) or {}),
        },
        "backfill": table_cfg.get("backfill", {}) or {},
        "output": {
            **(apis_root.get("output", {}) or {}),
            **(table_cfg.get("output", {}) or {}),
        },
        "multi_pulls": table_cfg.get("multi_pulls"),
        "join": table_cfg.get("join"),
    }

    env_cfg = expand_env_value(env_cfg)
    req_opts = expand_env_value(req_opts)
    api_eff = expand_env_value(api_eff)

    parse_cfg = table_cfg.get("parse", {}) or {"type": "json"}
    return env_cfg, api_eff, req_opts, parse_cfg
