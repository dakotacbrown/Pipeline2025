import os
import sys


def before_scenario(context, scenario):
    # container for active patches
    context.patches = []


def after_scenario(context, scenario):
    # Stop all patches
    for p in getattr(context, "patches", []):
        try:
            p.stop()
        except Exception:
            pass

    # Restore stdout
    if hasattr(context, "_old_stdout"):
        sys.stdout = context._old_stdout

    # Restore argv
    if hasattr(context, "_old_argv"):
        sys.argv = context._old_argv

    # Restore environ
    if hasattr(context, "_old_environ"):
        os.environ.clear()
        os.environ.update(context._old_environ)
