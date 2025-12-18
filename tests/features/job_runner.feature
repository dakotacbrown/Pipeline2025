Feature: Job runner component behavior

  Scenario: setup_path prefers an existing matching zip already on sys.path
    Given I have a sys.path list containing "/tmp/debi-etl-framework-glue-1.0.zip"
    When I call setup_path with pattern "debi-etl-framework-glue*.zip" (capturing errors)
    Then the first sys.path entry should be "/tmp/debi-etl-framework-glue-1.0.zip"
    And sys.path should contain "/tmp/debi-etl-framework-glue-1.0.zip/src"

  Scenario: setup_path returns when no zip exists on sys.path or filesystem
    Given I have an empty temp search directory
    And I have a sys.path list ["A", "B"]
    When I call setup_path with pattern "does-not-exist-*.zip"
    Then sys.path should remain ["A", "B"]

  Scenario: setup_path raises when more than one zip is found on filesystem
    Given I have a temp search directory with zips
      | name                               |
      | debi-etl-framework-glue-1.0.zip     |
      | debi-etl-framework-glue-2.0.zip     |
    And I have a sys.path list ["X"]
    When I call setup_path with pattern "debi-etl-framework-glue*.zip"
    Then setup_path should raise ValueError

  Scenario: main runs end-to-end with fake GithubConnection, logger, and api_wrapper
    Given the runner module is "src.run_step"
    And I install fake common modules and fake api_wrapper
    And I patch runner.setup_path to be a no-op
    When I run runner.main with argv
      | arg            | value                          |
      | --env          | dev                            |
      | --run_mode     | once                           |
      | --vendor       | acme                           |
      | --table        | users                          |
      | --event        | {"secret":"shh","x":1}         |
      | --file_path    | configs/foo.yml                |
      | --repo_name    | my-repo                        |
      | --github_token | ghp_123                        |
      | --start_date   | 2025-01-01                     |
      | --end_date     | 2025-01-31                     |
      | --log_level    | INFO                           |
      | --extra_env    | FOO=bar                        |
      | --extra_env    | HELLO=world                    |
      | --unknown_arg  | should_be_ignored              |
    Then stdout JSON should have status "ok"
    And run_ingester should be called with table "users" env "dev" run_mode "once"
    And run_ingester should receive start "2025-01-01" and end "2025-01-31"
    And GithubConnection should fetch file_path "configs/foo.yml"
    And environment variable "FOO" should equal "bar"
    And environment variable "HELLO" should equal "world"
