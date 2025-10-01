import logging
import os
import sys

import responses


def before_all(context):
    # Add project root to PYTHONPATH so steps can 'from api_ingestor import ApiIngestor'
    project_root = os.getenv(
        "PROJECT_ROOT", os.path.abspath(os.path.join(os.getcwd(), ".."))
    )
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    context.logger = logging.getLogger("behave.tests")
    context.logger.setLevel(logging.INFO)


def before_scenario(context, scenario):
    # Start a responses mock for each scenario
    context.responses = responses.RequestsMock(
        assert_all_requests_are_fired=False
    )
    context.responses.start()


def after_scenario(context, scenario):
    # Stop and reset mock
    context.responses.stop()
    context.responses.reset()
