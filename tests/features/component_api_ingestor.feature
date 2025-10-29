Feature: ApiIngestor component behavior
  Component-level tests for pagination modes, link expansion, and backfills
  using mocked HTTP endpoints.

  Background:
    Given a base API host "https://api.test/"

  Scenario: Cursor pagination combines pages until next token is missing
    Given a cursor endpoint "/cursor" with two pages of 2 items each
    And a cursor-table config
    When I run the ingestor once for table "cursor_table" in env "dev"
    Then the result has 4 rows

  Scenario: Page-number pagination increments until empty page
    Given a paged endpoint "/paged" where page 1 has 2 items and page 2 is empty
    And a page-table config
    When I run the ingestor once for table "page_table" in env "dev"
    Then the result has 2 rows

  Scenario: Link-header pagination follows rel="next"
    Given a link-header chain starting at "/link1" then "/link2"
    And a link-header-table config
    When I run the ingestor once for table "link_header_table" in env "dev"
    Then the result has 2 rows

  Scenario: Link expansion inherits session auth headers
    Given a base endpoint "/rows" that returns items with per-row detail URLs
    And detail endpoints require Authorization header
    And a link-expansion-table config with Authorization "Bearer TESTTOKEN"
    When I run the ingestor once for table "link_exp_table" in env "dev"
    Then the expanded result has 2 rows and includes the detail field "value"

  Scenario: Date-window backfill sends window params and concatenates
    Given a date-window endpoint "/events" that echoes window ranges as items
    And a date-backfill-table config with 2-day windows
    When I run the backfill for table "date_bf_table" in env "dev" from "2021-01-01" to "2021-01-03"
    Then the result has 3 rows and contains values "2021-01-01" "2021-01-02" "2021-01-03"

  Scenario: Salesforce SOQL-window backfill uses 'q' param then follows nextRecordsUrl
    Given a salesforce endpoint "/sf/query" that returns done=false then done=true
    And a soql-window-table config
    When I run the backfill for table "sf_soql_table" in env "dev" from "2021-01-01" to "2021-01-01"
    Then the result has 2 rows with ids "a" and "b"

  Scenario: Cursor backfill honors start_value and stops by max pages
    Given a cursor endpoint "/cursor" with two pages of 2 items each
    And a cursor-backfill-table config with start_value "s0" and max_pages 2
    When I run the backfill for table "cursor_bf_table" in env "dev" from "2021-01-01" to "2021-01-01"
    Then the result has 4 rows
