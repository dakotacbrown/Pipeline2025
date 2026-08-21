# Standalone per-table test data

`table_fixtures/*.jsonl` — 5 rows per table, one flat file per table,
using every column from the actual DDL spreadsheets (not just the
trimmed subset `gl_source_join.py`'s `EXPECTED_COLUMNS` reads). This is
meant to be more robust/realistic than the minimal fixtures in
`generate_fixtures.py` — closer to a real S3-landed JSONL dump.

## Scope

Covers the 9 tables `build_source_dataframe()` actually loads:

```
transaction_journal, invoice_line, invoice_line_tax,
payment_line_invoice_line, payment_line_invoice, credit_memo_line,
credit_memo_line_invoice_line, credit_memo_inv_application,
general_ledger_account
```

**Payment, CreditMemo, Invoice, Account, Refund, and Product2 are NOT
included** — `build_source_dataframe()` never loads those tables
directly (the header-level joins go straight from
`PaymentLineInvoice`/`CreditMemoInvApplication` to `InvoiceLine`, without
needing the `Payment`/`CreditMemo`/`Invoice` header objects themselves),
so fixtures for them wouldn't add anything toward testing the GL file
specifically. If you need those too, say so and I'll add them.

**Field names/types are transcribed from the DDL screenshots across our
conversation** — worth a spot-check against the real spreadsheets before
relying on this beyond local testing. The fields that actually matter to
the pipeline (join keys, `Business_Unit_BU__c`/`Department_ID_DID__c`,
`GL_Accounting_Number__c`, `ActivityDate`, `TransactionType`,
`Credit`/`Debit`, `Debit`/`CreditGeneralLedgerAccountId`) I'm confident
in; a few of the unused-by-the-pipeline filler fields were reconstructed
from memory and are lower-confidence.

## How the 5 rows are designed

The rows aren't random — they're cross-linked by ID so that running them
through the real pipeline exercises every resolution path in one pass:

| TransactionJournal row | Type | Proves |
|---|---|---|
| S1 | InvoiceLine | Direct resolution |
| S2 | InvoiceLineTax | One-hop indirect resolution (→ InvoiceLine) |
| S3 | Payment | Line-level (`PaymentLineInvoiceLine`) wins over header-level (`PaymentLineInvoice`) — a "trap" row exists that only the header-level path would reach |
| S4 | CreditMemo | Same line-level-wins-over-header-level proof, via `CreditMemoLine`/`CreditMemoLineInvoiceLine` vs. `CreditMemoInvApplication` |
| S5 | InvoiceLine | `GeneralLedgerAccount` GL_Accounting_Number__c `"10040049"` → `did` overrides to `"16605"` |

Each supporting table also carries a few additional filler rows (valid,
just not referenced by any TransactionJournal row) — this exercises that
the pipeline correctly *ignores* irrelevant rows too, not just correctly
resolves relevant ones.

## Running it

```
python3 generate_table_fixtures.py   # writes table_fixtures/*.jsonl
python3 run_table_fixtures_test.py   # stages them + runs the real pipeline
```

`run_table_fixtures_test.py` prints the joined dataframe, the full
fixed-width output, and four sanity checks:

```
IL3 (line-level Payment target)   bu=10904 present: True
IL4 (line-level CreditMemo target) bu=10905 present: True
IL5 (trap, should NEVER appear)    bu=99999 present: False
S5's did override to 16605 present: True
```

## Two real bugs this run caught

1. **My own fixture bug**: S5's `TransactionJournal.ReferenceTransactionRecordId`
   accidentally pointed at the same ID as the trap `InvoiceLine` (both
   were `IL5`), so S5 resolved to the trap's wrong `bu`/`did` (`99999`)
   instead of a real one. Fixed by pointing S5 at `IL1` instead — S5 is
   testing the GL-account override, not a new bu/did resolution path, so
   reusing an existing valid InvoiceLine is fine.
2. **A real pipeline bug in `fmt()`** (`salesforce_global_one.py`): the
   null check was `isinstance(value, float) and pd.isna(value)`, which
   only catches `float('nan')`. Now that `read_jsonl_from_s3()` reads
   every column as pandas' nullable `StringDtype`, a missing value is
   `pd.NA` — not a `float` — so the old check silently missed it. Before
   the fix, any legitimately-null field (e.g. `UsageType` on a
   Payment/CreditMemo transaction, which realistically has none) wrote
   the literal text `"<NA>"` into the fixed-width output instead of blank
   spaces. **Fixed** by using `pd.isna(value)` alone, which correctly
   catches `None`, `NaN`, `pd.NA`, and `pd.NaT` in one check. Regression
   tests: `test_pandas_na_becomes_blank_field`,
   `test_nat_becomes_blank_field` in `test_gl_journal_builder.py`.

Neither bug was visible in `generate_fixtures.py`'s original 11-scenario
demo, because those fixtures happened to always supply a real string (or
`None` via direct `pd.DataFrame` construction) for every optional field —
they never round-tripped genuinely-missing fields through the real
`read_jsonl_from_s3()` → `StringDtype` path the way these full-schema
fixtures do.
