# `salesforce_global_one.py` (GL Journal Entry Interface)

Also referred to in code comments and imports as `gl_journal_builder_pandas_s3.py`
— that's its filename on disk in some contexts; `salesforce_global_one.py` is
the name it's deployed/imported under.

## What this file does

Reads billing data that's already been joined and shaped by
`helpers/gl_source_join.py`, formats it into a fixed-width text file
matching the GL Journal Entry Interface spec, and submits that file to
OneStream via OneLake. It's the "last mile" of the pipeline: source data →
fixed-width file → OneLake submission. All of the Salesforce-object joins
and business-rule resolution (bu/did, GL account lookup, date filtering)
live upstream in `gl_source_join.py`; this file just formats and ships
whatever `gl_source_join.build_source_dataframe()` hands it.

It shares its outbound-write/validation/OneLake-submission step
(`write_and_submit_file`) with `salesforce_ofac.py` — see that function's
docstring in `helper_functions.py`.

## File structure, top to bottom

### `DECODE_METADATA`
A JSON blob describing the fixed-width layout to OneStream (field names,
byte positions, widths) for all four record types. This is submitted
alongside the file so OneStream knows how to decode it back into fields.
It was derived and verified against `build_gl_file()`'s actual field
layout below — the two must stay in sync. If a field width or position
changes in `journal_header()`/`journal_line()`/etc., `DECODE_METADATA`
needs the matching update, or OneStream will decode the file incorrectly
even though the file itself is well-formed.

### `choose_gl_identity(env)`
Returns the OneStream `schema_name` for this job, by environment. Mirrors
`salesforce_ofac.py`'s `choose_ofac_identity()` — same reasoning: this is
job-specific identity, not shared infrastructure, so it lives here rather
than in `helper_functions.py`.

Two things worth knowing:
- Unlike OFAC, there's no `source` value returned here. OFAC's `source` is
  only used for S3 file naming, not the Exchange submission payload
  itself (which is just `businessApplication`/`schemaName`/
  `fileSubmissions`). This job already has `filename_prefix` and
  `output_key_prefix` filling that naming role, so a GL `source` value
  wouldn't do anything.
- **The prod/qa values are placeholders** (`"PLACEHOLDER_GL_SCHEMA_PROD"` /
  `"PLACEHOLDER_GL_SCHEMA_QA"`), not confirmed real values. Fill these in
  once the GL journal's OneStream schema is registered — don't deploy
  with placeholders in place. `test_gl_journal_builder.py` has a test
  (`TestChooseGlIdentity.test_currently_placeholder_value_not_yet_real`)
  that fails on purpose until this is fixed.

### Fixed-width formatting helpers

| Function | Purpose |
|---|---|
| `fmt(value, length, justify, fill)` | Formats a single value to an exact field width — `None`/NaN become blank, values are truncated if too long, left/right-justified and padded otherwise. |
| `build_line(fields)` | Takes a list of `(value, length, justify, fill)` tuples and concatenates their `fmt()` output into one fixed-width line. |
| `file_header(creation_dt, transmit_id)` | Builds the `#H` record — file creation date/time + transmit ID. |
| `journal_header(business_unit, journal_date, source, description)` | Builds the `H` record — one per business unit per file. `journal_id` is hardcoded to `"NEXT"` and `avg_daily_balance_date`'s label is hardcoded to `"RECORDING"` — both are fixed spec values, not computed. |
| `journal_line(business_unit, account, dept_id, project_id, journal_line_ref, journal_line_desc, txn_currency_code, txn_monetary_amount)` | Builds one `L` record — one per transaction row. |
| `file_trailer(row_count, total_debits, total_credits, total_stat)` | Builds the `#T` record — row count and debit/credit/statistical totals, each formatted to 2 decimal places via `Decimal`/`ROUND_HALF_UP`. |

**`journal_line()`'s `ledger` field is hardcoded to `"CORP"`.** There used
to be a `startswith("US")` check meant to distinguish `"CORP"` vs
`"LOCAL"`, but real business units are numeric (e.g. `"10901"`), not
`"US"`/`"EU"`-prefixed strings, so that check never matched anything and
every line silently fell through to one branch. `CORP` is a stand-in
until there's a real rule for distinguishing CORP vs LOCAL by numeric BU
— **this is an open item, not a final decision.**

### `build_gl_file(df, business_unit, source, creation_dt)`

Assembles the complete file: `file_header` → one or more
`journal_header`/`journal_line...` blocks → `file_trailer`.

**One journal header per (business unit, activity date) pair — not per
business unit.** Per the real GL Journal File Layout spec, `journal_header`'s
Journal Date field is "Transaction Date from Source" — a single date, not
a range. A business unit whose transactions span multiple days in a run's
date window (month-to-date, by default) gets one header per day, each
carrying only that day's lines. `journal_header`'s date comes from each
row's own `TransactionJournal.ActivityDate` — not `creation_dt` (a
mismatch that existed until this was corrected).

**`business_unit=None` (default):** covers every `(business_unit,
activity_date)` pair present in `df`. This is the "Multi-Record Fixed
Width" structure from the spec.
**`business_unit="US001"` (a specific value):** every row is labeled with
this BU regardless of its own `InvoiceLine.Business_Unit` value — `run()`
already filters `df` to one BU before calling this when that's wanted, so
this parameter is a label override for direct/manual calls, not a filter.
Still split into one header per distinct `activity_date` within it.

**Journal Date resolution, per row:** `TransactionJournal.ActivityDate` if
present and non-null, else `creation_dt`'s date (same
"`Table.Column`, else a manual-call fallback" pattern used for the amount
field). A direct/manual call that never supplies `ActivityDate` at all
still gets exactly one header per BU, dated by `creation_dt` — unchanged
from the original single-header-per-BU behavior.

**Format warning — deliberately different, easy to transpose:** the file
header's `creation_date` is `YYYYMMDD`; every journal header's
`journal_date` is `MMDDYYYY`. Both are computed independently
(`file_header()` vs. `journal_header()`), so a future format change to
either one needs to be checked against the other, not assumed to match.

**Row-level logic, per transaction:**
1. Pulls the amount from `TransactionJournal.CreditDebit` (what
   `gl_source_join.build_source_dataframe()` produces) or
   `txn_monetary_amount` (for direct/manual calls that skip the source
   join entirely).
2. Sign determines debit vs. credit bucket for the trailer totals
   (`amt >= 0` → debit, else → credit).
3. Builds the journal line via a `field <- Table.Column` mapping — see the
   table below. Every field checks a short-name key first (`"account"`,
   `"dept_id"`, etc.) before falling back to the `Table.Column`-named key,
   so this function works both with `gl_source_join`'s output and with a
   more manually-constructed row.

**Field mapping (row → `journal_line()` argument):**

| `journal_line()` arg | Primary key checked | Fallback key | Notes |
|---|---|---|---|
| `account` | `account` | `GeneralLedgerAccount.GL_Accounting_Number__c` | No cleaning/stripping applied — this used to come from `Account.AccountNumber` with a leading `"A"` stripped (`clean_account_number()`, since deleted from `gl_source_join.py` — confirmed unused anywhere once the GL pipeline stopped calling it), but that was an Account-ID-specific quirk that doesn't apply to `GL_Accounting_Number__c`. |
| `dept_id` | `dept_id` | `InvoiceLine.Department_Id` | bu default (`"10901"`) and did overrides (slingshot/databolt/`10040049`→`16605`) are already applied upstream, in `gl_source_join.apply_did_overrides()` — nothing left to do here. |
| `project_id` | `project_id` | *(none)* | Left blank — no source field mapped yet. |
| `journal_line_ref` | `journal_line_ref` | `TransactionJournal.UsageType` | **Tentative** — marked with a `?` in earlier design notes, not fully confirmed. |
| `journal_line_desc` | `journal_line_desc` | `TransactionJournal.TransactionType` | **Tentative**, same caveat as above. |
| `txn_currency_code` | `txn_currency_code` | *(none)* | No source field mapped yet; always blank unless passed directly. |

**`header_description` is hardcoded to `"RevCloud Batch"`** — a
placeholder. Open question: what should this be when one business unit's
batch spans multiple distinct `TransactionJournal.Name` values? Not yet
decided.

### `build_filename(prefix, creation_dt)`
`{prefix}_{YYYYMMDDHHMMSS}.txt`. `prefix` must be exactly 3 characters
(e.g. `"BX1"`) — raises `ValueError` otherwise.

### `run(...)` — orchestration
The non-Databricks-specific entry point: builds the source dataframe,
optionally filters to one business unit, builds the file content, then
writes + submits it. Called by `main()`, but also directly testable/
callable without any of `main()`'s Databricks-environment setup.

Key parameters:
- `s3_client` must already be authenticated (via
  `new_session(service_credential).client("s3")`) — not a bare
  `boto3.client("s3")`.
- `writer_config` needs `ba`/`schema_name`/`iam_role`/`base_url`/`env`/
  `region`. `bucket` and `file_name` get overwritten inside
  `write_and_submit_file` regardless of what's already in the dict.
- `source_prefix` is the S3 prefix each Salesforce dataset_id folder sits
  under — comes from the job YAML's `source_key_prefix` parameter as a
  config value (not assembled from vendor/segment guesses in code),
  specifically because a wrong prefix guess fails *silently* (empty but
  valid → 0 rows returned) rather than erroring.
- `start_date`/`end_date` are optional `YYYY-MM-DD` strings passed
  straight through to `gl_source_join.build_source_dataframe()` (see
  `gl_source_join.resolve_date_window()`). Both omitted → month-to-date.
- `business_unit=None` (default) means the output file covers **all**
  business units in one multi-record file. Passing a specific value
  filters to just that BU. Which of these should be the actual production
  behavior is still an open decision, not finalized.
- The main output file and the validation file share the same
  `year=/month=/day=/hour=/` partition (computed once from this run's
  `creation_dt`), so a given run's two outputs are always easy to find
  together.

### `main()` — the Databricks entry point
Deferred imports (inside the function body, not at module level) for
`asvc1scoredataservices_common`, `pyspark`, and `helpers.helper_functions`
— these only exist in the real Databricks environment, so importing them
at module load time would break anything trying to import this module
outside that environment (e.g. tests).

**Argv layout** (9 required, 2 optional trailing):

| Index | Name | Required? |
|---|---|---|
| 0 | `env` | Yes |
| 1 | `chamber_role` | Yes |
| 2 | `service_credential` | Yes |
| 3 | `bucket` | Yes |
| 4 | `output_key_prefix` | Yes |
| 5 | `validation_key_prefix` | Yes |
| 6 | `filename_prefix` | Yes |
| 7 | `source_key_prefix` | Yes |
| 8 | `validation_file_type` | Yes |
| 9 | `start_date` | No — omit for month-to-date |
| 10 | `end_date` | No — omit for month-to-date |

The two date args are trailing and optional specifically so existing job
YAML invocations (which only pass the 9 required args) keep working
without modification.

**What `main()` does, in order:**
1. Validates argv length, raises with a usage message if too short.
2. Resolves `schema_name` via `choose_gl_identity(env)` (see the
   placeholder-value caveat above).
3. Retrieves AWS credentials via `new_session(service_credential)`, then
   builds an S3 client with **explicit connect/read timeouts** — a bare
   `boto3` client has no default timeout, so a stuck network/NCC
   connectivity issue would hang indefinitely instead of failing fast
   with a clear error.
4. Resolves the shared Exchange OAuth endpoint/IAM role/base URL via
   `choose_exchange_env(env)` — the **same Exchange app registration**
   `salesforce_ofac.py` uses (confirmed, not per-job config), so it's one
   shared source of truth rather than two YAML files that could drift.
5. Retrieves Exchange client ID/secret from Chamber, exchanges them for an
   OAuth token.
6. Calls `run(...)`, passing all the above through.
7. On success: returns `{"status_code": 200, "s3_url", "num_records",
   "message": "SUCCESS"}`.
8. On any exception: logs the error with a full stack trace, then
   re-raises a generic wrapped exception (the original error detail lives
   in the log, not the re-raised message).
9. In `finally`: always writes an execution log entry to S3 (via
   `write_execution_log_to_s3`), regardless of success or failure —
   includes `final_state`, `failure_message`, `records_published`,
   timestamps, etc.

`business_application` (`"BAC1SCOREDATASERVICES"`) and `region`
(`"us-west-2"`) are hardcoded, matching `salesforce_ofac.py`'s own
`WRITER_CONFIG` literals — these are shared-infrastructure values, not
job-specific parameters.

## Known open items / not-yet-decided

These aren't bugs — they're places where a deliberate placeholder or
default was chosen pending a follow-up decision. Listed here instead of
inline so they don't get lost when comments are trimmed:

1. **`choose_gl_identity()`'s prod/qa schema names are placeholders.** Do
   not deploy until the real OneStream schema is registered and these are
   filled in.
2. **`journal_line()`'s `ledger` is always `"CORP"`.** No rule yet for
   distinguishing CORP vs. LOCAL by numeric business unit.
3. **`journal_line_ref`/`journal_line_desc`'s source fields
   (`TransactionJournal.UsageType`/`.TransactionType`) are tentative**,
   not fully confirmed against the spec.
4. **`header_description` is always `"RevCloud Batch"`.** Open question
   for when a BU's batch spans multiple `TransactionJournal.Name` values.
5. **`build_gl_file()`'s `business_unit=None` vs. a specific value** —
   which should be the real production default is still an open decision.
6. **`project_id` and `txn_currency_code` have no source field mapped** —
   always blank unless passed directly.

## Related files
- `helpers/gl_source_join.py` — builds the source dataframe this file
  consumes (all Salesforce joins, bu/did resolution, GL account lookup,
  date filtering). See its own module docstring and
  `docs/gl_source_join.md` (if present) for that logic.
- `helpers/helper_functions.py` — `write_and_submit_file()`,
  `choose_exchange_env()`, `new_session()`, `retrieve_oauth_token()`,
  `build_execution_log_s3_path()` — shared with `salesforce_ofac.py`.
- `test_gl_journal_builder.py` — full test coverage for every function in
  this file, including the placeholder-schema-name test mentioned above.
