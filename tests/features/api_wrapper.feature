@component
Feature: API wrapper

Scenario: run_ingester in once mode calls ApiIngester.run_once
  Given a valid wrapper config and event
  When run_ingester is called in "once" mode
  Then ApiIngester.run_once is invoked
  And the wrapper returns metadata

Scenario: run_ingester in backfill mode calls ApiIngester.run_backfill
  Given a valid wrapper config and event
  When run_ingester is called in "backfill" mode with dates
  Then ApiIngester.run_backfill is invoked

Scenario: run_ingester backfill without dates raises an error
  Given a valid wrapper config and event
  When run_ingester is called in "backfill" mode without dates
  Then a ValueError is raised

