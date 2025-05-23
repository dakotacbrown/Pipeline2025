import sys
from logging import Logger
from pathlib import Path

import yaml

"""
Config
Handles the loading of environment-specific configurations from a file, which are
used to configure ETL processes. Provides tools to ensure that config files
are loaded correctly and their contents accessible.
"""


class ConfigReader:
    def __init__(self, log: Logger, configs_path: Path) -> None:
        """Initializes the Config object with a configurations file path and a logger.

        :param configs_path: Path to the configurations file.
        :param log: Logger instance for logging messages.
        """
        self.configs_path = configs_path
        self.configs_data = None
        self.log = log

    def load_configurations(self) -> "ConfigReader":
        """Loads configurations from the specified file into the configs_data attribute.

        :return: Self for fluent interface.
        :raises SystemExit: If the file cannot be loaded or does not exist.
        """
        try:
            self._check_path_exists()
            try:
                with open(self.configs_path, "rb") as configs_file:
                    self.configs_data = yaml.safe_load(configs_file)
                return self
            except Exception as e:
                self.log.error(
                    "Issue loading file '%s': %s" % (self.configs_path, e)
                )
                sys.exit(1)
        except FileNotFoundError as e:
            self.log.error("Issue loading file: %s" % (e))
            sys.exit(1)

    def _check_path_exists(self) -> None:
        """Checks if the Config file exists at the specified path.

        :raises FileNotFoundError: If the configurations file does not exist.
        """
        if not self.configs_path.exists():
            raise FileNotFoundError(
                "The file '%s' does not exist." % (self.configs_path)
            )
