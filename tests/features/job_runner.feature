@component
Feature: Glue job runner

  Scenario: job runner calls wrapper and prints JSON
    Given valid Glue CLI arguments
    When the job runner is executed
    Then run_ingester is called
    And a success JSON is printed
