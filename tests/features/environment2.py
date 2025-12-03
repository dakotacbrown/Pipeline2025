import os
import sys


def before_scenario(context, scenario):
    # container for active patches
    context.patches = []


def after_scenario(context, scenario):
    # Stop all patches (if any)
    for p in getattr(context, "patches", []):
        try:
            p.stop()
        except Exception:
            pass

    # Restore stdout if this scenario changed it
    if "_old_stdout" in context:
        sys.stdout = context._old_stdout

    # Restore argv if this scenario changed it
    if "_old_argv" in context:
        sys.argv = context._old_argv

    # Restore environ if this scenario changed it
    if "_old_environ" in context:
        os.environ.clear()
        os.environ.update(context._old_environ)
