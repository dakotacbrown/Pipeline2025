Feature: API ingester wrapper
  The API wrapper should orchestrate environment setup and call ApiIngester.

  Scenario: run_ingester in once mode calls ApiIngester.run_once
    Given a basic API config and event
    When I call run_ingester in "once" mode
    Then ApiIngester run_once is called
    And run_ingester returns the meta rows

  Scenario: run_ingester in backfill mode calls ApiIngester.run_backfill
    Given a basic API config and event
    When I call run_ingester in "backfill" mode
    Then ApiIngester run_backfill is called

  Scenario: run_ingester backfill without start/end raises an error
    Given a basic API config and event
    When I call run_ingester in "backfill" mode without dates
    Then run_ingester raises a ValueError
