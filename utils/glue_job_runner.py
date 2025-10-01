# run_step.py
import os
import sys
import json
from pathlib import Path

# In the current working directory, find a list of files that match
# the following pattern "ingestor_bundle..." with a ".zip"
files = list(Path.cwd().rglob("ingestor_bundle*.zip"))


def setup_path():
    """
    Set this up as a function so we can test it.

    This function checks for a zip file used by glue and adds it to the sys.path list.
    """
    # In the current working directory, find a list of files that match
    # the following pattern "ingestor_bundle..." with a ".zip"
    files = list(Path.cwd().rglob("ingestor_bundle*.zip"))

    if len(files) > 1:
        raise ValueError(
            f"More than one ingestor_bundle zip found: {files}"
        )
    elif len(files) == 1:
        zip_file_with_zip = files[0].name
        # Remove the ".zip" extension
        zip_file_without_zip = zip_file_with_zip.split(".zip")[0]
        # Create two strings with that name
        base_path = f"{zip_file_with_zip}/{zip_file_without_zip}/"
        python_path = f"{zip_file_with_zip}/{zip_file_without_zip}/Python"

        # Add those two paths to the sys.path list
        sys.path.insert(0, base_path)
        sys.path.insert(0, python_path)

        base_path = f"{zip_file_with_zip}/"
        python_path = f"{zip_file_with_zip}/Python"

        # Add those two paths to the sys.path list
        sys.path.insert(0, base_path)
        sys.path.insert(0, python_path)
    else:
        # We don't test for 0 files because it would error out
        # when running tests.
        pass


# Call the function we just created (this mirrors the screenshot exactly)
setup_path()

# Import after sys.path was extended so it can be found inside the ZIP
from ingestor_wrapper import run_ingestor  # noqa: E402


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "y", "on")


def main_with_args() -> None:
    """
    Read env vars and invoke the ingestor wrapper. Prints the metadata JSON.
    Env vars:
      TABLE_NAME (required)
      ENV_NAME (required)
      YAML_PATH (optional, path inside the ZIP; wrapper can default if missing)
      BACKFILL (optional: true/false)
      START_DATE (required iff BACKFILL=true, YYYY-MM-DD)
      END_DATE   (required iff BACKFILL=true, YYYY-MM-DD)
      LOG_LEVEL  (optional; default INFO)
    """
    table = os.environ.get("TABLE_NAME")
    env = os.environ.get("ENV_NAME")
    yaml_path = os.environ.get("YAML_PATH")
    backfill = _env_bool("BACKFILL", default=False)
    start = os.environ.get("START_DATE")
    end = os.environ.get("END_DATE")
    log_level = os.environ.get("LOG_LEVEL", "INFO")

    if not table or not env:
        print("ERROR: TABLE_NAME and ENV_NAME must be set.", file=sys.stderr)
        sys.exit(2)

    if backfill and (not start or not end):
        print("ERROR: BACKFILL=true requires START_DATE and END_DATE.", file=sys.stderr)
        sys.exit(3)

    meta = run_ingestor(
        yaml_path=yaml_path,
        table=table,
        env=env,
        backfill=backfill,
        start=start,
        end=end,
        log_level=log_level,
    )
    print(json.dumps(meta, separators=(",", ":"), sort_keys=True))


def main() -> None:
    main_with_args()


if __name__ == "__main__":  # pragma: no cover
    main()
