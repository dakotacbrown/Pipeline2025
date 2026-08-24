# GL journal builder changes — summary

Test results: **288 passed, 0 failed.** Fully green.
Baseline (unmodified files, before any of this work): 241 passed, 0 failed.

## Files changed
- `gl_source_join.py` (blank-string bu/did normalization; `build_reference_to_account_lookup()`/`clean_account_number()` deleted)
- `gl_journal_builder_pandas_s3.py` (Journal Date now per-transaction, header grouping by (BU, date); `fmt()` null-check fix — see below)
- `s3_utils.py` (StringDtype on every JSON-read column — see below)
- `test_gl_source_join.py`
- `test_gl_journal_builder.py`
- `test_s3_utils.py`

`helper_functions.py`, `onelake_writer.py`, `salesforce_ofac.py`,
`salesforce_ofac_billing_preview.py`, and their test files are **untouched**.

## What changed and why

### 1. Account → GeneralLedgerAccount
- Added `general_ledger_account` to `DATASET_IDS` / `EXPECTED_COLUMNS`.
- New `resolve_gl_accounting_number(tj, general_ledger_account)`: resolves
  `gl_accounting_number_c` via `TransactionJournal.DebitGeneralLedgerAccountId`
  or `.CreditGeneralLedgerAccountId` (whichever is populated) →
  `GeneralLedgerAccount.Id` → `GeneralLedgerAccount.GL_Accounting_Number__c`.
- `build_source_dataframe()` no longer loads `account` or calls
  `build_reference_to_account_lookup()` / `clean_account_number()`.
- **`build_reference_to_account_lookup()` and `clean_account_number()` have
  now been deleted from `gl_source_join.py` entirely**, along with their
  tests. Per Dakota, this account-number code is no longer needed. (An
  earlier version of this change kept them defined-but-unused out of
  caution, reasoning they might still be needed by `salesforce_ofac.py`
  — that was wrong: neither OFAC file ever called either function, only
  `DATASET_IDS["account"]` directly, for a separate, unrelated
  Account/BillingAccount join. Confirmed via grep before deleting.)
- **`DATASET_IDS["account"]` itself was kept**, not removed —
  `salesforce_ofac.py` and `salesforce_ofac_billing_preview.py` both still
  read it directly for that separate join and would break if it were
  removed.
- The final output column `Account.AccountNumber` is now
  `GeneralLedgerAccount.GL_Accounting_Number__c`. In
  `gl_journal_builder_pandas_s3.py`, `build_gl_file()`'s account field
  mapping was updated to match, with no leading-character stripping (that
  was an Account-ID-specific quirk that doesn't apply here).

### 2. bu/did overrides
New `apply_did_overrides(df)`, applied after `resolve_bu_did()`, in
priority order:
1. `bu` defaults to `"10901"` wherever it's null.
2. `did` → `"16637"` if `InvoiceLine.Name` contains "slingshot"
   (case-insensitive), or `"16635"` if it contains "databolt".
3. `did` → `"16605"` whenever `gl_accounting_number_c == "10040049"` —
   **highest priority**, overrides both of the above.

This is GL-pipeline-specific; `salesforce_ofac.py` doesn't call it.

`InvoiceLine.Name` now flows through `resolve_bu_did()` as
`InvoiceLineName` on the output (renamed to avoid colliding with
`TransactionJournal.Name`), added defensively — if `invoice_line` doesn't
have a `Name` column at all, it comes back null instead of raising.

### 3. Date window
New `resolve_date_window(start_date=None, end_date=None)`: bounds
`TransactionJournal.ActivityDate` (a full datetime in Salesforce, e.g.
`"2026-08-12T17:47:25.000+0000"`, confirmed via your screenshot — not a
plain date). Defaults to month-to-date (midnight UTC on the 1st through
now); pass both `start_date`/`end_date` as `YYYY-MM-DD` for an explicit
window (passing only one raises).

Applied in `build_source_dataframe()` right after `transaction_journal`
loads, before any joins. Threaded through as new optional kwargs on
`build_source_dataframe()` → `run()` → `main()`. In `main()`, they're two
new **optional trailing** positional CLI args (`argv[9]`, `argv[10]`), so
existing job YAML invocations with exactly the 9 required args are
unaffected.

## Before you deploy
Nothing outstanding — `general_ledger_account`'s dataset_id is filled in
with the real value you gave me (`78e7dd64-9999-42f1-a44a-38bf5157375e`)
and the suite is fully green.

## Two real bugs found by end-to-end testing (see `e2e_demo/`)

Building realistic fixture data and running the actual pipeline against
it (rather than mocked-out internals) surfaced two real, compounding bugs
— **now fixed**, in `s3_utils.py`:

1. **`read_jsonl_from_s3()` silently corrupted numeric-looking string
   columns.** `pd.read_json(..., lines=True)` — the default — converts any
   column where every value looks like a plain integer (business unit
   codes, department IDs, GL account numbers) into `int64`. Confirmed this
   drops leading zeros (`"00450"` → `450`). Same class of bug as the
   `CaseNumber` leading-zero fix already applied elsewhere in this
   pipeline (`dtype=str` on `pd.read_csv`).
2. **Caused by #1: the `10040049` → `16605` override was silently dead
   code.** `apply_did_overrides()` compares `gl_accounting_number_c ==
   "10040049"` as a string. With bug #1 in place, that column came back as
   `int64`, so the comparison was always `False` — the highest-priority
   override in the entire bu/did chain never actually fired, even though
   every unit test for it passed (hand-built test `DataFrame`s never
   round-trip through `pd.read_json`, so they never hit the coercion).

**Follow-up, per Dakota's explicit direction:** the initial fix
(`dtype=False`) only stopped pandas from coercing numeric-*looking*
strings — it didn't touch columns that are natively JSON booleans or
numbers (e.g. `"IsDeleted": false`, `"Amount": 100.5`), which still came
back as native `bool`/`float64` without an explicit cast. Per Dakota:
"Everything from the json files should be viewed as dtype string and
utf-8 encoding" — the DDL spreadsheets define each column's real,
eventual type, and applying that is each downstream consumer's job
(`pd.to_numeric()`, `pd.to_datetime()`, `Decimal(str(...))` — already the
pattern used throughout this pipeline), not the reader's. `read_jsonl_from_s3()`
now does `.astype("string")` (pandas' nullable `StringDtype`, not python
`str`) on every column after reading — `StringDtype` specifically because
it preserves real JSON nulls as `pd.NA` instead of stringifying them into
the literal text `"None"`/`"nan"`.

One caveat confirmed and documented in the function's own docstring: a
`==` comparison against a `StringDtype` column with `pd.NA` present
returns `pd.NA` for those rows (not `False`) — confirmed this is still
safe for `.loc[mask, ...]` boolean-mask assignment (pandas treats `pd.NA`
there like `False`), but would NOT be safe if that comparison result were
ever used in a plain Python `if`.

All of this is covered by regression tests in `test_s3_utils.py`
(`test_all_columns_are_pandas_string_dtype`,
`test_native_json_boolean_is_stringified_not_left_as_bool`,
`test_native_json_number_is_stringified_not_left_as_float`,
`test_null_stays_a_real_null_not_stringified_to_none_text`,
`test_downstream_numeric_conversion_still_works_on_string_dtype`,
`test_downstream_date_conversion_still_works_on_string_dtype`) and
`test_gl_source_join.py`
(`test_caveat_int_typed_gl_account_number_does_not_trigger_override`).
See `e2e_demo/SCENARIOS_README.md` for the original bug writeup and how
to reproduce.

## Journal header now grouped by (business unit, date), not just business unit

Per the real GL Journal File Layout spec (confirmed via screenshot):
`journal_header`'s Journal Date field is "**Transaction Date from
Source**" — a single date, not a range. The file previously emitted one
journal header per business unit covering its entire date range in one
block, using the file's build time as the date (wrong field entirely).
Now:

- `journal_header`'s date comes from each transaction's own
  `TransactionJournal.ActivityDate`, not `creation_dt`.
- `build_gl_file()` groups by **(business_unit, activity_date)** — a
  business unit with transactions spanning 3 distinct dates in the run's
  window now produces 3 separate journal headers, each with only that
  day's lines under it. Per Dakota, this applies across the whole
  date window a run covers (month-to-date by default), not just a single
  day.
- The file header's own `creation_date`/`creation_time` are **unrelated**
  and unchanged — those still reflect when the file was actually built.
  Worth double-checking if either format ever changes: the file header
  uses `YYYYMMDD`, the journal header uses `MMDDYYYY` — different order,
  easy to transpose.
- Direct/manual calls to `build_gl_file()` that don't supply
  `TransactionJournal.ActivityDate` at all still work exactly as before —
  falls back to `creation_dt`'s date, so all such rows land under one
  header per BU, same as prior behavior.

See `e2e_demo/SCENARIOS_README.md` for what this looks like against
realistic data (business unit `10902` splits into 4 separate headers).

## bu/did already sourced from InvoiceLine columns first — confirmed, plus one real gap fixed

You asked me to confirm: bu/did should come from InvoiceLine's own
`Business_Unit_BU__c`/`Department_ID_DID__c` columns first, falling back
to the existing default/override resolution only when those are null.
Confirmed — that's exactly what `resolve_bu_did()` already does; every
resolution path (direct, tax, payment header, credit memo header) pulls
`bu`/`did` from InvoiceLine's own two columns and nowhere else.

**One real gap found and fixed:** the null-check only caught true
`NaN`/`None`, not a blank/empty string `""` (which Salesforce can return
for an optional text field instead of a true null). Confirmed with you
this should get the same treatment. `apply_did_overrides()` now
normalizes blank/whitespace-only `bu`/`did` strings to a real null before
the `10901` default and override logic run.

**A second, unrelated bug surfaced while implementing that fix:**
normalizing via a whole-column `.apply()`/`.map()` silently re-infers an
all-null `object`-dtype column as `float64` (same class of pandas
dtype-inference surprise as the earlier JSON-reading bug) — which then
broke the `did` string overrides (`TypeError: Invalid value '16637' for
dtype 'float64'`) on any row where InvoiceLine never resolved at all.
Fixed by using a targeted `.loc` assignment on only the blank cells
instead of rebuilding the whole column. Regression test:
`test_blank_bu_in_an_entirely_unresolved_row_does_not_raise`.

## `build_reference_to_account_lookup()` / `clean_account_number()` deleted

Confirmed unused anywhere (only ever called by this file's own
`build_source_dataframe()`, which stopped calling them earlier in this
change) — per Dakota, "the account number code should no longer be
needed." Deleted both functions and their tests from `gl_source_join.py`
/ `test_gl_source_join.py`. `DATASET_IDS["account"]` itself stays — see
the file's module docstring for why (OFAC's separate, unrelated
Account/BillingAccount join still needs it).

## New standalone per-table test data (`e2e_demo/table_fixtures/`)

5 full-DDL-width rows per table, one flat `.jsonl` file per table (not
nested in the dataset_id folder structure), for the 9 tables
`build_source_dataframe()` loads. See `e2e_demo/TABLE_FIXTURES_README.md`
for the full scenario design and how to run it.

**Two more real bugs caught by actually running these through the real
pipeline** (not just checking the JSON is well-formed):

1. My own fixture bug — S5 accidentally pointed at the same InvoiceLine
   ID as a deliberate "trap" row, resolving to the wrong `bu`/`did`.
   Fixed in the fixture data.
2. **A real pipeline bug** in `salesforce_global_one.py`'s `fmt()`: the
   null check (`isinstance(value, float) and pd.isna(value)`) only
   caught `float('nan')`, not `pd.NA` — which is exactly what a missing
   value looks like now that every column is `StringDtype`. Before the
   fix, a legitimately-null field (e.g. `UsageType` on a Payment
   transaction) wrote the literal text `"<NA>"` into the fixed-width
   output instead of blank spaces. **Fixed** by using `pd.isna(value)`
   alone. Regression tests: `test_pandas_na_becomes_blank_field`,
   `test_nat_becomes_blank_field`.

## `e2e_demo/` — new end-to-end demo

A self-contained demo that generates realistic fixture data (11 labeled
scenarios covering every resolution path, override, and edge case
discussed in this conversation) and runs the real pipeline against it —
not mocks. Run with `python3 e2e_demo/run_e2e_test.py`. See
`e2e_demo/SCENARIOS_README.md` for what each scenario proves and the
expected output to check it against.

## Also worth confirming
- `GeneralLedgerAccount.GL_Accounting_Number__c` as the exact field name —
  I matched it against the DDL screenshot's row 20 (`GL_Accounting_Number__c`,
  varchar, nullable, 255), but flagging since it's the one field name in
  this change I didn't get from your own code, only from a screenshot.
- The `10040049` / `16605` / `16637` / `16635` values are used as
  plain-string comparisons (`==`/`.str.contains`) — if any of these are
  ever meant to be numeric-typed on the Salesforce side, worth double
  checking the comparison still holds.

## New TransactionType support: DebitMemoLine, RefundLinePayment, Refund

Three new TransactionType resolution paths added to `resolve_bu_did()`:

- **DebitMemoLine** — fully live, real dataset_id, single-hop resolution
  via `ReferenceRecordId` -> InvoiceLine (3 rows in prod).
- **RefundLinePayment** / **Refund** — structurally complete, same
  line-level-tried-first / header-level-fallback shape as Payment, via an
  explicit `Payment` join hop (per Dakota: "keep payment just to make sure
  there's nothing lost in the joins"). Gated on `refund_line_payment`'s
  dataset_id, still a PLACEHOLDER as of this note — one intentional test
  failure (`test_refund_line_payment_dataset_id_still_needs_real_value`)
  keeps this visible until it's filled in.

New standalone JSONL fixtures for all four new tables
(`debit_memo_line`, `payment`, `refund`, `refund_line_payment`) added to
`e2e_demo/table_fixtures/`, deliberately cross-linked to existing fixtures
so they resolve through real working chains, not just well-formed JSON.
Three new scenarios (S12–S14) added to `run_table_fixtures_test.py`'s
transaction journal fixture to exercise all three paths end to end.

## Product2 as the preferred bu/did source

Per Dakota: "it's better to go through product2 than invoice line for the
bu and did fields, however it's a good fallback as well." Universal
across every TransactionType — Product2's `Business_Unit_BU__c`/
`Department_ID_DID__c` are now tried first; InvoiceLine's own bu/did
(the previous sole source) is the fallback, used only where Product2Id
doesn't resolve or Product2's fields come back null. `bu` and `did` fall
back independently of each other, not as an all-or-nothing pair.

CreditMemoLine and DebitMemoLine both carry their own `Product2Id`
directly, letting Product2 resolution skip `CreditMemoLineInvoiceLine`
(the junction table with an unconfirmed row count) or the nullable
`ReferenceRecordId` hop entirely — the specific case Dakota called out
as the motivation: "especially if any of these junction tables are
empty."

**A real bug was found and fixed during this work**: the initial
CreditMemoLine/DebitMemoLine direct-Product2Id merge only *enriched*
rows that already existed in the resolution lookup from some other path
— a left-merge can only enrich existing rows, it can't create new ones.
That silently dropped exactly the case this feature was built for: a
CreditMemo with NO match in *either* `CreditMemoLineInvoiceLine` *or*
`CreditMemoInvApplication`. Confirmed directly by testing that exact
scenario before fixing it. Fixed by adding a second pass that appends a
standalone row for any Product2Id that never got a row through any other
path. Regression test:
`test_credit_memo_line_provides_product2id_when_both_junction_paths_empty`.

`product2`'s dataset_id is confirmed real (`29351664-7f3c-4266-8937-018cc5a7dd44`).

New test coverage: `TestDirectProduct2IdWithoutInvoiceLine`,
`TestResolveProduct2BuDid`, `TestApplyProduct2Priority` — all previously
referenced in code comments but never actually written.

The fan-out caveat (an Invoice with multiple InvoiceLines spanning more
than one bu/did — header-level resolution takes the first match) was
explicitly confirmed acceptable by Dakota ("just pull everything in...
it's better to have one row for everything") — documentation updated
accordingly rather than left as an open risk.

## Journal line field mapping fix

`journal_line_ref` now sources from `TransactionJournal.Name` (was
`UsageType`, marked tentative). `journal_line_desc` sourced from
`TransactionJournal.TransactionType` — already coded this way, now
confirmed correct rather than tentative. Both confirmed directly by
Dakota. New tests:
`test_journal_line_reference_sources_from_transaction_journal_name`,
`test_journal_line_description_sources_from_transaction_type`.

## Reference docs

- `docs/gl_journal_source_join_validation.sql` — SQL mirror of
  `gl_source_join.py`, updated for all of the above (new CTE 1b for
  Product2, three new TransactionType CTEs, updated fan-out caveat
  language, updated readiness-check queries).
- `docs/ddl_reference/salesforce_general_ledger_schema.md` — full
  transcription of all 15 tables from Dakota's DDL spreadsheet (previously
  only had one table transcribed).
- `e2e_demo/xlsx_work/gl_jrnl_file_layout_005_updated.xlsx` — mapping
  sheet updated for the journal line field mapping fix and Product2
  priority.

---

# Major rewrite: Product2 as the SOLE bu/did source (supersedes the section above)

The section above described Product2 as *preferred*, with InvoiceLine as
a fallback. That two-tier design was replaced entirely, per Dakota:
"Product2 should not have a null or blank did/bu... it's okay to remove
the invoice line resolution." InvoiceLine's own
`Business_Unit_BU__c`/`Department_ID_DID__c`/`Name` fields are no longer
read for bu/did purposes at all — `apply_product2_priority()`'s
`combine_first` fallback is gone.

## Full TransactionType coverage — all 15, not 7

The `TransactionType` picklist has 15 confirmed values (per Dakota's
screenshots of the field's full `picklistValues`). All 15 now have a
resolution path, up from the original 7:

`InvoiceLine`, `InvoiceLineTax`, `DebitMemoLine`, `Payment`, `CreditMemo`,
`RefundLinePayment`, `Refund` (original 7) — plus new: `Invoice`,
`CreditMemoLine`, `CreditMemoLineTax`, `PaymentLineInvoice`,
`PaymentLineInvoiceLine`, `CreditMemoInvApplication`,
`CreditMemoLineInvoiceLine`, `DebitMemoLineTax`.

Two new tables added: `credit_memo_line_tax` (real dataset_id, was
present in the very first `ingest_revcloud.yml` screenshot, just never
wired up) and `debit_memo_line_tax` (PLACEHOLDER — no corresponding table
exists in Salesforce yet, per Dakota; built anyway, "I'll still need to
have the mapping in case it goes live").

## Function renames (reflecting the simplified role)

- `resolve_bu_did()` → **`resolve_product2_id()`** — now resolves ONLY
  `Product2Id` per TransactionType; no longer outputs `bu`/`did`/
  `InvoiceLineName` at all.
- `resolve_product2_bu_did()` → **`resolve_product2_fields()`** — now
  also resolves `product2_name` (for the slingshot/databolt check, moved
  off `InvoiceLine.Name` entirely per Dakota: "we shouldn't need invoice
  line name anymore for the name check").
- `apply_product2_priority()` → **`apply_product2_bu_did()`** — no more
  priority merge, just a direct assignment (`tj["bu"] = tj["product2_bu"]`).

## New: `validate_required_tables_present()`

Per Dakota: "If the transaction [journal], product2, general ledger
account tables, or any table under a distinct list from transaction type
are empty it should fail. That would mean a data issue is present."

- `transaction_journal`, `product2`, `general_ledger_account` are
  unconditionally mandatory — empty means raise, always.
- Every other table's requirement depends on which `TransactionType`
  values are actually present in a given run's data — and, critically,
  **a type only fails if EVERY one of its possible resolution paths is
  dead**, not if just one of several fallbacks is empty. Confirmed
  directly with Dakota: `PaymentLineInvoiceLine` has been 0 rows this
  whole project — that's known, expected state, not a data issue, since
  `Payment`'s header-level path resolves fine on its own. Getting this
  OR-path design wrong (requiring every table in every path) would have
  meant failing on literally every real run.

`REQUIRED_TABLES_BY_TRANSACTION_TYPE` is the single source of truth for
this — used by both the validation function and as living documentation
of exactly which tables back each type.

## Real bugs found and fixed during this rewrite

1. **`s3_utils.py` — empty/0-byte JSONL files broke downstream joins.**
   `pd.read_json("", lines=True)` returns a DataFrame with **zero
   columns**, not just zero rows — a different, silently-broken shape
   than "no S3 objects found" (which the code already handled). Any
   `df["SomeColumn"]` access on such a frame raised `KeyError`. Confirmed
   directly by testing the exact scenario before fixing. Fixed in
   `read_jsonl_prefix_from_s3()` — reshapes to `expected_columns` when
   every file under a prefix is empty, not just when there are no files
   at all. 4 new regression tests.

2. **`Payment`'s own resolution was incorrectly gated on the `Payment`
   table.** `TransactionType="Payment"`'s `ReferenceTransactionRecordId`
   already IS the `PaymentId` directly — it never needed confirming
   against a separately-loaded `Payment` table. That confirmation hop was
   only ever meant for `RefundLinePayment`/`Refund` (per Dakota: "keep
   payment just to make sure there's nothing lost in the joins"). Caught
   via a real end-to-end run against fixture data (unit tests happened to
   include a matching `Payment` row and missed it) — S3's scenario
   pointed at a `PaymentId` that genuinely wasn't in `payment.jsonl`, and
   the resolution silently fell to the `10901` default instead of the
   correct `bu`/`did`. Fixed; regression test added.

3. **`CreditMemo`'s "line-level via `CreditMemoLineInvoiceLine`" path was
   structurally dead code.** It derived from the same `CreditMemoLine`
   table, keyed by the same `CreditMemoId`, as the "direct" candidate,
   which is always tried first. Since the dedup keeps the first candidate
   regardless of whether its value is null, the junction-table path could
   never actually change the result — not even in the one case (a null
   `CreditMemoLine.Product2Id` with a real answer reachable via the
   junction) where it would have mattered. Confirmed by constructing that
   exact scenario directly. This was a real, working candidate *before*
   the Product2 rewrite (it existed to reach `InvoiceLine`'s own bu/did,
   which the pre-rewrite "direct" candidate couldn't do) — the rewrite
   silently made it redundant. Removed, along with the now-inaccurate
   `REQUIRED_TABLES_BY_TRANSACTION_TYPE` entry that claimed the path was
   viable. `CreditMemo` now resolves via 2 paths (direct, header), not 3.

4. **The `IL5` "trap" fixture's `99999` marker went silently inert.**
   Once `InvoiceLine.Business_Unit_BU__c` stopped being read at all, the
   trap InvoiceLine's `99999` value could never surface through the
   pipeline regardless of whether the line-level-vs-header-level priority
   logic was still correct — the "`99999` must never appear" check would
   have started passing trivially, not because the logic worked, but
   because `99999` had no path to the output anymore. Fixed by giving the
   trap's `Product2` record (`PROD05AAA`) a real `99999`/`99999` value,
   restoring the trap's actual detection power.

## Dead code / dead tests removed (per Dakota: "using only what we need")

- `DATASET_IDS["invoice"]`/`["credit_memo"]` (+ their `EXPECTED_COLUMNS`
  entries) — leftovers from the deleted `build_reference_to_account_lookup()`,
  confirmed unused anywhere (not the GL pipeline, not OFAC) via a direct
  cross-reference of every `load()` call against every config key.
- The internal `.empty` guards inside `il_product2()`/`cml_product2()` —
  proven structurally unreachable: `invoice_line`/`credit_memo_line`
  always arrive with the correct column shape via the `EXPECTED_COLUMNS`
  fallback even when empty (0 rows), so selecting columns from them never
  raises regardless of row count. Confirmed empirically before removing.
- `test_account_no_longer_loaded_by_build_source_dataframe` — added zero
  coverage (the behavior was already exercised elsewhere) and tested an
  internal implementation detail (which dataset_ids got requested) via
  monkeypatching, not any observable output.
- `test_no_logging_when_log_is_none` — fully redundant; every other
  orchestration test already implicitly covers `log=None` by never
  passing `log` at all.
- Two separate logging tests consolidated into one, checking the `if
  log:` branch fires without pinning every message's exact wording
  (implementation detail, not meaningful behavior).

Result: **222 statements, 99% coverage** (up from 229 statements / 94%
before this cleanup) — coverage went up while the statement count went
*down*, since the improvement came from deleting unreachable code, not
padding tests around it. The one remaining uncovered block is the
`if __name__ == "__main__":` standalone entry point, which requires real
boto3/S3 credentials and is intentionally left untested by unit tests.

## Test suite state

**334 passing, 2 intentional failures** (`refund_line_payment` and
`debit_memo_line_tax` dataset_id placeholders — same "fails on purpose
until the real value lands" pattern used throughout this project).

## Docs updated to match (this pass)

- `docs/gl_journal_source_join_validation.sql` — full rewrite: Product2
  as sole source, all 15 TransactionType CTEs, `credit_memo_via_line`
  removed, new CTE 7 mirroring `validate_required_tables_present()`.
- `e2e_demo/xlsx_work/build_mapping_sheet.py` /
  `gl_jrnl_file_layout_005_updated.xlsx` — `TransactionType to BU-DID`
  sheet fully rebuilt (single Product2Id-path column, all 15 types, no
  more "7 unhandled" section); `Mapping - Source to File` sheet's bu/did
  rows updated to drop "PREFERRED/fallback" language.
- `docs/ddl_reference/salesforce_general_ledger_schema.md` — coverage
  table updated to all-15-confirmed, stale `resolve_bu_did()` references
  fixed, CreditMemo's removed path noted.
- `e2e_demo/TABLE_FIXTURES_README.md` — scope section corrected (Product2/
  Payment/Refund ARE included, contradicting an earlier claim), new
  `product2.jsonl` section documenting the trap fix, three more real bugs
  documented.
- `e2e_demo/generate_new_table_fixtures.py`,
  `generate_product2_fixtures.py`, `generate_table_fixtures.py` — stray
  `resolve_bu_did()` references in comments corrected;
  `generate_product2_fixtures.py`'s trap product (`PROD05AAA`) given a
  real `99999`/`99999` record instead of being omitted (see bug #4 above).

