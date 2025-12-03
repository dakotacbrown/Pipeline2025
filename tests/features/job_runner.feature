Feature: Glue job runner
  The Glue job runner script should wire CLI args to the wrapper and print JSON.

  Scenario: Running the job runner once calls the wrapper and prints status JSON
    Given a basic Glue event
    When I run the job runner in "once" mode
    Then run_ingester is called with the expected arguments
    And the job runner prints a successful status JSON
