import os

from behave import then


@then('environment variable "{key}" should equal "{value}"')
def step_env_var_equals(context, key, value):
    assert (
        os.environ.get(key) == value
    ), f"Expected {key}={value}, got {os.environ.get(key)!r}"
