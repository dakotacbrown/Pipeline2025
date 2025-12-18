import os
import sys


def before_scenario(context, scenario):
    context._orig_environ = dict(os.environ)
    context._inserted_modules = []


def after_scenario(context, scenario):
    # restore environ
    os.environ.clear()
    os.environ.update(context._orig_environ)

    # remove any fake modules we inserted
    for name in reversed(context._inserted_modules):
        sys.modules.pop(name, None)
