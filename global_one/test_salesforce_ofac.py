"""
Tests for salesforce_ofac.py's choose_ofac_identity(). Only this one
function is covered here — salesforce_ofac.py calls setup_logger() at
module level (not deferred inside main()), so importing it at all requires
a working logger already patched in first. See the fixture below.

Run with: pytest test_salesforce_ofac.py -v
"""

import pytest
from unittest.mock import MagicMock


@pytest.fixture
def ofac_module(monkeypatch):
    """
    Patches setup_logger before importing salesforce_ofac.py, since
    `logger = setup_logger()` runs at module level. Returns the imported
    module.
    """
    monkeypatch.setattr(
        "asvc1scoredataservices_common.logger.basic_logger.setup_logger",
        MagicMock(return_value=MagicMock()),
    )
    import importlib
    import src.salesforce.resources.scripts.salesforce_ofac as ofac
    importlib.reload(ofac)  # in case an earlier test already imported it pre-patch
    return ofac


class TestChooseOfacIdentity:
    def test_returns_source_and_schema_name_tuple(self, ofac_module):
        source, schema_name = ofac_module.choose_ofac_identity("qa")
        assert source == "c1s_ofac_sanctions_reporting"
        assert schema_name == "c1s_ofac_sanctions_reporting_v2"

    def test_prod_source_matches_qa_source(self, ofac_module):
        # confirmed real values: source is identical between envs, only
        # schema_name differs (see the cos_/c1s_ prefix note in the
        # function's docstring)
        prod_source, _ = ofac_module.choose_ofac_identity("prod")
        qa_source, _ = ofac_module.choose_ofac_identity("qa")
        assert prod_source == qa_source == "c1s_ofac_sanctions_reporting"

    def test_prod_and_qa_schema_name_differ(self, ofac_module):
        _, prod_schema = ofac_module.choose_ofac_identity("prod")
        _, qa_schema = ofac_module.choose_ofac_identity("qa")
        assert prod_schema != qa_schema
        assert prod_schema == "cos_ofac_sanctions_reporting_v2"
        assert qa_schema == "c1s_ofac_sanctions_reporting_v2"

    def test_invalid_env_raises(self, ofac_module):
        with pytest.raises(ValueError, match="Invalid environment"):
            ofac_module.choose_ofac_identity("staging")
