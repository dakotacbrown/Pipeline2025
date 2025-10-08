import types
from logging import Logger

import pytest

from utils.basic_logger import setup_logger


# ----- simple logger used across tests -----
class Log:
    def info(self, *a, **k):
        pass

    def error(self, *a, **k):
        pass


@pytest.fixture
def logger(log):  # some tests import 'logger'
    return log


# ----- lightweight HTTP fakes -----
class FakeResponse:
    def __init__(self, *, json_data=None, text="", headers=None, links=None):
        self._json = json_data
        self.content = (text or "").encode("utf-8")
        self.headers = headers or {}
        self.links = links or {}
        self.status_code = 200

    def json(self):
        return self._json

    def raise_for_status(self):
        return


class FakeSession:
    """
    mapping = {
      "https://api/a": [FakeResponse(...), FakeResponse(...)],
      "https://api/b": [FakeResponse(...)]
    }
    """

    def __init__(self, mapping):
        self._m = {k: list(v) for k, v in mapping.items()}
        self.headers = {}
        self.proxies = {}
        self.verify = True
        self.auth = None

    def get(self, url, **kw):
        q = self._m.get(url, [])
        return q.pop(0) if q else FakeResponse(json_data={})


@pytest.fixture
def fake_sess():
    return FakeSession


# ----- ctx fixture shared by helper tests -----
@pytest.fixture
def ctx(log):
    from api_ingestor import parsing

    return {
        "log": log,
        "table": "t",
        "env": "e",
        "expansion_session_ids": set(),
        "current_output_ctx": {},
        "flush_seq": 0,
        # default parsing hooks (tests may override)
        "to_dataframe": parsing.to_dataframe,
        "json_obj_to_df": parsing.json_obj_to_df,
        "drop_keys_any_depth": parsing.drop_keys_any_depth,
    }


# ----- stub boto3 for output tests (and ApiIngestor end-to-end) -----
@pytest.fixture
def patch_boto3(monkeypatch):
    uploads = []

    class Client:
        def put_object(self, Bucket, Key, Body, **kw):
            uploads.append({"Bucket": Bucket, "Key": Key, "Body": Body, **kw})

    class Session:
        def __init__(self, region_name=None):
            pass

        def client(self, name, endpoint_url=None):
            return Client()

    monkeypatch.setitem(
        __import__("sys").modules,
        "boto3",
        types.SimpleNamespace(session=types.SimpleNamespace(Session=Session)),
    )
    return uploads


# Handy to_dataframe override for pagination tests
@pytest.fixture
def to_dataframe(monkeypatch):
    from api_ingestor import parsing

    return parsing.to_dataframe


@pytest.fixture(scope="session", autouse=True)
def log() -> Logger:
    log = setup_logger()
    return log
