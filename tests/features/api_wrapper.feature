Feature: API wrapper component behavior

  Scenario: run_ingester once sets tokens and calls ApiIngester.run_once
    Given the wrapper module is "src.api_wrapper"
    And I install fake common modules for the wrapper
    And I patch requests.post for oauth to return tokens
    When I call run_ingester with parameters
      | table    | env | run_mode | start | end |
      | users    | dev | once     |       |     |
    And the wrapper event is
      """
      {
        "c1_oauth_url": "https://oauth.example/c1",
        "exchange_headers": {"h": "1"},
        "exchange_data": {"d": "2"},
        "env_vars": {"FOO": "bar"}
      }
      """
    And the wrapper config is
      """
      {"envs":{"dev":{}},"apis":{"users":{}}}
      """
    Then C1_OAUTH_TOKEN should equal "C1_TOKEN"
    And environment variable "ENV" should equal "dev"
    And environment variable "TABLE" should equal "users"
    And ApiIngester should run_once with table "users" env "dev"
    And environment variable "FOO" should equal "bar"
    And requests.post should be called 1 times

  Scenario: run_ingester sets env vars from event env_vars scoped to env
    Given the wrapper module is "src.api_wrapper"
    And I install fake common modules for the wrapper
    And I patch requests.post for oauth to return tokens
    When I call run_ingester with parameters
      | table | env  | run_mode | start | end |
      | users | prod | once     |       |     |
    And the wrapper event is
      """
      {
        "c1_oauth_url": "https://oauth.example/c1",
        "env_vars": {
          "dev": {"FOO": "dev"},
          "prod": {"FOO": "prod", "HELLO": "world"}
        }
      }
      """
    And the wrapper config is
      """
      {"envs":{"prod":{}},"apis":{"users":{}}}
      """
    Then environment variable "FOO" should equal "prod"
    And environment variable "HELLO" should equal "world"

  Scenario: run_ingester retrieves data oauth token only when data_headers and data_auth are present
    Given the wrapper module is "src.api_wrapper"
    And I install fake common modules for the wrapper
    And I patch requests.post for oauth to return tokens
    When I call run_ingester with parameters
      | table | env | run_mode | start | end |
      | users | dev | once     |       |     |
    And the wrapper event is
      """
      {
        "c1_oauth_url": "https://oauth.example/c1",
        "data_auth_url": "https://oauth.example/data",
        "data_headers": {"h":"x"},
        "data_auth": {"a":"y"}
      }
      """
    And the wrapper config is
      """
      {"envs":{"dev":{}},"apis":{"users":{}}}
      """
    Then DATA_OAUTH_TOKEN should equal "DATA_TOKEN"
    And requests.post should be called 2 times

  Scenario: run_ingester backfill parses dates and calls run_backfill
    Given the wrapper module is "src.api_wrapper"
    And I install fake common modules for the wrapper
    And I patch requests.post for oauth to return tokens
    When I call run_ingester with parameters
      | table | env | run_mode  | start      | end        |
      | users | dev | backfill  | 2025-01-01 | 2025-01-31 |
    And the wrapper event is
      """
      {
        "c1_oauth_url": "https://oauth.example/c1"
      }
      """
    And the wrapper config is
      """
      {"envs":{"dev":{}},"apis":{"users":{}}}
      """
    Then environment variable "START_DATE" should equal "2025-01-01"
    And environment variable "END_DATE" should equal "2025-01-31"
    And ApiIngester should run_backfill with table "users" env "dev" start "2025-01-01" end "2025-01-31"

  Scenario: backfill requires both start and end
    Given the wrapper module is "src.api_wrapper"
    And I install fake common modules for the wrapper
    And I patch requests.post for oauth to return tokens
    When I call run_ingester expecting ValueError with parameters
      | table | env | run_mode | start      | end |
      | users | dev | backfill | 2025-01-01 |     |
    And the wrapper event is
      """
      {"c1_oauth_url": "https://oauth.example/c1"}
      """
    And the wrapper config is
      """
      {"envs":{"dev":{}},"apis":{"users":{}}}
      """
    Then a ValueError should have been raised

  Scenario: backfill validates date format
    Given the wrapper module is "src.api_wrapper"
    And I install fake common modules for the wrapper
    And I patch requests.post for oauth to return tokens
    When I call run_ingester expecting ValueError with parameters
      | table | env | run_mode | start      | end        |
      | users | dev | backfill | 2025/01/01 | 2025-01-31 |
    And the wrapper event is
      """
      {"c1_oauth_url": "https://oauth.example/c1"}
      """
    And the wrapper config is
      """
      {"envs":{"dev":{}},"apis":{"users":{}}}
      """
    Then a ValueError should have been raised

  Scenario: required parameters table and env are enforced
    Given the wrapper module is "src.api_wrapper"
    And I install fake common modules for the wrapper
    And I patch requests.post for oauth to return tokens
    When I call run_ingester expecting ValueError with parameters
      | table | env | run_mode | start | end |
      |       | dev | once     |       |     |
    And the wrapper event is
      """
      {"c1_oauth_url": "https://oauth.example/c1"}
      """
    And the wrapper config is
      """
      {"envs":{"dev":{}},"apis":{"users":{}}}
      """
    Then a ValueError should have been raised
