# End-to-end demo — scenario guide

Run with: `python3 run_e2e_test.py` (from inside `e2e_demo/`)

This generates realistic JSONL fixture data (shaped exactly like the real
S3 bucket: `salesforce/reports/{dataset_id}/data.jsonl`), runs the REAL
`gl_source_join.build_source_dataframe()` and
`salesforce_global_one.build_gl_file()` — not mocked-out internals, the
actual production code — against it through a small fake S3 client, and
writes out everything to `output/`:

- `01_joined_dataframe.csv` — what `build_source_dataframe()` produced
- `02_gl_journal_file.txt` — the actual fixed-width file
- `03_decoded_breakdown.txt` — the fixed-width file decoded back into
  labeled fields (using the real `DECODE_METADATA`), so you can check
  field alignment by eye without counting characters
- `04_run_log.txt` — every `log.info()` message from the run

## The 11 test scenarios

Window used: `start_date="2026-08-01"`, `end_date="2026-08-20"`.

| # | `TransactionJournal.Name` | Proves | Expected `bu` | Expected `did` | Expected amount |
|---|---|---|---|---|---|
| S1 | Direct InvoiceLine | Plain InvoiceLine → InvoiceLine resolution | `10902` | `20500` | `1500.00` (debit) |
| S2 | Payment header fallback | Payment → PaymentLineInvoice → Invoice → InvoiceLine | `10902` | `20500` | `-750.00` (credit) |
| S3 | CreditMemo header fallback | CreditMemo → CreditMemoInvApplication → Invoice → InvoiceLine | `10902` | `20500` | `-200.00` (credit) |
| S4 | Unresolvable, bu defaults to 10901 | No matching InvoiceLine anywhere → bu default | `10901` | *(null)* | `50.00` (debit) |
| S5 | Slingshot product name override | `InvoiceLine.Name` contains "Slingshot" → did override | `10903` | `16637` | `300.00` (debit) |
| S6 | DataBolt product name override | `InvoiceLine.Name` contains "DataBolt" → did override | `10903` | `16635` | `400.00` (debit) |
| S7 | 10040049 override beats Slingshot override | GL account `10040049` override wins over the product-name override, even though the name also says "Slingshot" | `10903` | **`16605`** (not 16637) | `999.00` (debit) |
| S8 | Outside date window, should be excluded | Dated July 15 — outside the Aug 1–20 window | *(row absent entirely)* | | |
| S9 | Second business unit | Proves multi-BU grouping — a distinct `journal_header` block | `20450` | `40500` | `250.00` (debit) |
| S10 | InvoiceLineTax indirect resolution | InvoiceLineTax → InvoiceLine, one hop | `10902` | `20500` | `-45.00` (credit) |
| S11 | Line-level payment wins over header-level | Both a line-level (`PaymentLineInvoiceLine`) and header-level (`PaymentLineInvoice`) match exist for the same payment — line-level must win | `10904` | `50500` | `600.00` (debit) |

**S11 detail:** two `InvoiceLine` rows share the same `InvoiceId` (`INV011`)
— `IL011A` (bu=`10904`, did=`50500`, the correct line-level target) and
`IL011B` (bu=`99999`, did=`99999`, a deliberately-wrong "trap" that only
the header-level fallback would reach). If line-level priority ever broke,
this scenario would show `99999`/`99999` instead — easy to spot.

## What to check in the output

1. **Row count**: `build_source_dataframe()` should return **10 rows**
   (11 scenarios in, S8 filtered out by date). The run log's
   `filtering transaction_journal to ActivityDate ...complete (10 rows)`
   line confirms this before any joins even run.
2. **S7's `did` is `16605`, not `16637`.** This is the highest-priority
   override and the one most worth double-checking by eye in
   `01_joined_dataframe.csv`.
3. **S4's `did` and GL account are both null**, but `bu` is `10901` (the
   default) and the amount still resolves independently — it flows into a
   real `journal_line` in the output file with a blank `department_id`
   field.
4. **One journal header per (business unit, date) pair, not one per
   business unit.** BU `10902` has transactions on 4 distinct dates (S1,
   S2, S3, S10) and correctly produces 4 separate `H` records — one per
   date, each carrying only that day's lines. BU `10903` (S5/S6/S7) gets
   3. The actual run produced exactly this: 10 headers total (4+3+1+1+1
   across the 5 business units), each `journal_date` matching its group's
   `TransactionJournal.ActivityDate`, formatted `MMDDYYYY`.
5. **`journal_date` format vs. the file header's `creation_date` format —
   these are deliberately different and easy to transpose.** The file
   header (`#H` line) uses `YYYYMMDD` (`20260820`); every journal header
   (`H` line) uses `MMDDYYYY` (e.g. `08052026` for Aug 5, 2026). Both are
   visible side by side in `02_gl_journal_file.txt`.
6. **The trailer totals** in `03_decoded_breakdown.txt` (`total_debits` /
   `total_credits`) should equal the sum of all positive / negative
   amounts above: debits = `1500+50+300+400+999+250+600 = 4099.00`,
   credits = `-750-200-45 = -995.00`. The actual run produced exactly
   these values.
7. **`row_count` in the trailer** should be journal headers + journal
   lines = 10 headers (one per BU/date pair, see #4) + 10 transaction
   lines = **20**. The actual run produced `000000020`.
8. **Field alignment** in `03_decoded_breakdown.txt` — e.g. for S1's line,
   `journal_account` should read `'20011111  '` and `department_id`
   should read `'20500     '`, cleanly isolated with no bleed from
   neighboring fields.

## Two real bugs this run caught

Neither of these was visible in the existing mocked unit test suite,
because hand-built test `DataFrame`s never round-trip through
`pd.read_json` and so never hit the coercion described below. Both are
now fixed in `s3_utils.py`, with regression tests added in
`test_s3_utils.py` and `test_gl_source_join.py`. See `CHANGES_SUMMARY.md`
for the exact diff.

**Bug 1 — leading-zero / type corruption on ingest.** `pd.read_json(...,
lines=True)` silently converts a column to `int64` if every value in it
happens to look like a plain integer (e.g. `Business_Unit_BU__c: "10902"`).
Confirmed this drops leading zeros too (`"00450"` → `450`). This is the
same class of bug as the `CaseNumber` leading-zero issue already fixed
elsewhere in this pipeline via `dtype=str` on `pd.read_csv` — same root
cause, different reader. Fixed by adding `dtype=False` to
`read_jsonl_from_s3()`'s `pd.read_json()` call.

**Bug 2 (caused by Bug 1) — the `10040049` override silently never
fired.** `apply_did_overrides()` compares
`gl_accounting_number_c == "10040049"` as a string. Before the fix,
`GL_Accounting_Number__c` came back as `int64` from S3, so that comparison
was silently always `False` — the highest-priority override in the whole
bu/did resolution chain was dead code in practice. This is exactly what
S7 in this demo is built to catch: before the fix, S7 showed `did=16637`
(the Slingshot override winning instead); after the fix, it correctly
shows `did=16605`.
