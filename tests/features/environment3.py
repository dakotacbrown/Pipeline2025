import os
import sys


def before_scenario(context, scenario):
    context._orig_environ = dict(os.environ)
    context._inserted_modules = []
    context._orig_sys_modules = set(sys.modules.keys())


def after_scenario(context, scenario):
    # restore environ
    os.environ.clear()
    os.environ.update(context._orig_environ)

    # remove any fake modules we inserted
    for name in reversed(context._inserted_modules):
        sys.modules.pop(name, None)

    # Best-effort cleanup: remove any modules imported during scenario (avoids leakage)
    added = [m for m in sys.modules.keys() if m not in context._orig_sys_modules]
    for m in reversed(added):
        sys.modules.pop(m, None)
