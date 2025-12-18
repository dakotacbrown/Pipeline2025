import os
import sys
from pathlib import Path


def before_all(context):
    # Ensure repo root is importable (so `import src.run_step` works)
    repo_root = Path.cwd()
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))


def before_scenario(context, scenario):
    context._orig_environ = dict(os.environ)
    context._inserted_modules = []


def after_scenario(context, scenario):
    # Restore env vars
    os.environ.clear()
    os.environ.update(context._orig_environ)

    # Remove any fake modules inserted during scenario
    for name in reversed(getattr(context, "_inserted_modules", [])):
        sys.modules.pop(name, None)
