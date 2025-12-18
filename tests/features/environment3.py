import os
import sys


def before_scenario(context, scenario):
    context._orig_environ = dict(os.environ)
    context._orig_sys_path = list(sys.path)
    context._inserted_modules = []


def after_scenario(context, scenario):
    # restore env
    os.environ.clear()
    os.environ.update(context._orig_environ)

    # restore sys.path
    sys.path[:] = context._orig_sys_path

    # remove faked modules
    for name in reversed(getattr(context, "_inserted_modules", [])):
        sys.modules.pop(name, None)

    # close temp dirs if your steps stored them
    td = getattr(context, "_tmpdir", None)
    if td is not None:
        try:
            td.cleanup()
        except Exception:
            pass
