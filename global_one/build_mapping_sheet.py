"""
Adds a "Mapping - Source to File" sheet to the GL Journal File Layout
workbook, showing exactly what gl_journal_builder_pandas_s3.py /
gl_source_join.py populate each field with today — Table.Column, a
hardcoded standard value, or blank — plus a status flag for anything
that's a placeholder, tentative, or an open decision rather than a
confirmed real mapping.

The original "File Layout - RECORDING" sheet is left completely
untouched — this only ADDS a new sheet, matching its font (Arial),
header fill style, and column-width conventions.
"""

import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

SRC = "/mnt/user-data/uploads/gl_jrnl_file_layout_005.xlsx"
OUT = "/home/claude/repo/e2e_demo/xlsx_work/gl_jrnl_file_layout_005_updated.xlsx"

FONT_NAME = "Arial"
TITLE_FONT = Font(name=FONT_NAME, size=14, bold=True)
SECTION_FONT = Font(name=FONT_NAME, size=10, bold=True)
HEADER_FONT = Font(name=FONT_NAME, size=10, bold=True, color="FFFFFF")
BODY_FONT = Font(name=FONT_NAME, size=10)
NOTE_FONT = Font(name=FONT_NAME, size=9, italic=True)

HEADER_FILL = PatternFill("solid", fgColor="404040")
SECTION_FILL = PatternFill("solid", fgColor="BFBFBF")
CONFIRMED_FILL = PatternFill("solid", fgColor="C6EFCE")   # green — matches source directly
STANDARD_FILL = PatternFill("solid", fgColor="D9D9D9")    # gray — hardcoded/blank by spec, no mapping needed
TENTATIVE_FILL = PatternFill("solid", fgColor="FFEB9C")   # yellow — marked "?" in design notes
OPEN_FILL = PatternFill("solid", fgColor="FFC7CE")        # red — placeholder / open decision / gap

STATUS_FILL = {
    "CONFIRMED": CONFIRMED_FILL,
    "STANDARD": STANDARD_FILL,
    "TENTATIVE": TENTATIVE_FILL,
    "OPEN": OPEN_FILL,
}

THIN = Side(style="thin", color="B7B7B7")
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)

COLS = ["Field No.", "Field Name", "Position", "Length", "Current Source / Value", "Intermediary Joins", "Status", "Notes"]
COL_WIDTHS = [10, 26, 10, 8, 38, 45, 13, 65]

# ---------------------------------------------------------------------------
# Table 1: TransactionType -> Product2 -> bu/did
# All 15 confirmed picklist values, per Dakota's screenshots of the
# field's full picklistValues. Product2 is the SOLE bu/did source now —
# no more separate "InvoiceLine fallback" column, that layer is gone
# (per Dakota: "it's okay to remove the invoice line resolution").
# ---------------------------------------------------------------------------

T1_COLS = ["TransactionType", "Path to Product2Id", "Row Count Evidence", "Status"]
T1_WIDTHS = [22, 75, 45, 13]

TXN_TYPE_ROWS = [
    ("InvoiceLine",
     "InvoiceLine.Product2Id directly",
     "N/A — direct match",
     "CONFIRMED"),
    ("InvoiceLineTax",
     "InvoiceLineTax.InvoiceLineId -> InvoiceLine.Product2Id (one hop)",
     "N/A — direct match",
     "CONFIRMED"),
    ("DebitMemoLine",
     "DebitMemoLine.Product2Id directly",
     "DebitMemoLine CONFIRMED 3 rows (per Dakota)",
     "CONFIRMED"),
    ("Payment",
     "ReferenceTransactionRecordId = Payment.Id directly (does NOT require a matching row "
     "in the Payment table itself — confirmed via a real end-to-end run against fixture "
     "data that gating this on Payment.Id was a bug, since fixed) -> "
     "(a) PaymentLineInvoiceLine.PaymentId -> InvoiceLineId -> InvoiceLine.Product2Id "
     "[line-level, 0 rows, dormant] OR (b) PaymentLineInvoice.PaymentId -> InvoiceId -> "
     "InvoiceLine.InvoiceId -> InvoiceLine.Product2Id [header-level, what resolves today]",
     "PaymentLineInvoiceLine: 0 rows. PaymentLineInvoice: CONFIRMED 864 rows",
     "CONFIRMED (via header path)"),
    ("CreditMemo",
     "(a) DIRECT — CreditMemoLine.CreditMemoId = ReferenceTransactionRecordId -> "
     "CreditMemoLine.Product2Id (bypasses the junction table entirely) OR (b) header — "
     "CreditMemoInvApplication -> InvoiceLine.Product2Id. NOTE: there is deliberately NO "
     "\"line-level via CreditMemoLineInvoiceLine\" path — confirmed structurally DEAD "
     "(it derived from the same CreditMemoLine table, keyed by the same CreditMemoId, as "
     "the direct candidate, which always wins the dedup regardless of null value — so it "
     "could never actually change the result). Removed, not kept as unreachable code.",
     "CreditMemoLine: CONFIRMED 149 rows (per Dakota). CreditMemoInvApplication: "
     "CONFIRMED 42 rows",
     "CONFIRMED"),
    ("RefundLinePayment",
     "Direct entry (RefundLinePayment.Id) -> Payment (explicit join hop, per Dakota: "
     "\"keep payment just to make sure there's nothing lost in the joins\" — specific to "
     "RefundLinePayment/Refund, NOT to Payment's own resolution above) -> same (a)/(b) "
     "split as Payment above",
     "RefundLinePayment: dataset_id still PLACEHOLDER, per Dakota — resolves to 0 rows "
     "against real data until registration completes",
     "OPEN — blocked on dataset_id"),
    ("Refund",
     "Refund.Id -> RefundLinePayment.RefundId (one hop earlier than RefundLinePayment "
     "above) -> Payment -> same (a)/(b) split",
     "Same PLACEHOLDER block as RefundLinePayment above",
     "OPEN — blocked on dataset_id"),
    ("Invoice",
     "Invoice.Id (= ReferenceTransactionRecordId directly, no separate Invoice table load "
     "needed) -> InvoiceLine.InvoiceId -> InvoiceLine.Product2Id",
     "N/A — direct match via InvoiceLine",
     "CONFIRMED"),
    ("CreditMemoLine",
     "Direct entry (CreditMemoLine.Id) -> CreditMemoLine.Product2Id",
     "N/A — direct match",
     "CONFIRMED"),
    ("CreditMemoLineTax",
     "CreditMemoLineTax.CreditMemoLineId -> CreditMemoLine.Id -> CreditMemoLine.Product2Id",
     "credit_memo_line_tax dataset_id CONFIRMED real (per Dakota)",
     "CONFIRMED"),
    ("PaymentLineInvoice",
     "Direct entry (PaymentLineInvoice.Id) -> PaymentLineInvoice.InvoiceId -> "
     "InvoiceLine.InvoiceId -> InvoiceLine.Product2Id",
     "N/A — direct match via InvoiceLine",
     "CONFIRMED"),
    ("PaymentLineInvoiceLine",
     "Direct entry (PaymentLineInvoiceLine.Id) -> PaymentLineInvoiceLine.InvoiceLineId -> "
     "InvoiceLine.Id -> InvoiceLine.Product2Id",
     "N/A — direct match via InvoiceLine",
     "CONFIRMED"),
    ("CreditMemoInvApplication",
     "Direct entry (CreditMemoInvApplication.Id) -> CreditMemoInvApplication.CreditMemoId "
     "-> CreditMemoLine (via CreditMemoLine.CreditMemoId) -> CreditMemoLine.Product2Id",
     "N/A — direct match via CreditMemoLine",
     "CONFIRMED"),
    ("CreditMemoLineInvoiceLine",
     "Direct entry (CreditMemoLineInvoiceLine.Id) -> "
     "CreditMemoLineInvoiceLine.CreditMemoLineId -> CreditMemoLine.Id -> "
     "CreditMemoLine.Product2Id",
     "N/A — direct match via CreditMemoLine",
     "CONFIRMED"),
    ("DebitMemoLineTax",
     "DebitMemoLineTax.DebitMemoLineId -> DebitMemoLine.Id -> DebitMemoLine.Product2Id",
     "NO CORRESPONDING TABLE EXISTS IN SALESFORCE YET, per Dakota — built anyway "
     "(\"I'll still need to have the mapping in case it goes live\")",
     "OPEN — no table exists yet"),
]

TXN_TYPE_PRIORITY_NOTE = (
    "After the per-TransactionType resolution above finds a Product2Id, TWO more steps "
    "apply universally to every row, in order:\n\n"
    "1. Product2 lookup: whatever Product2Id was found -> Product2.Business_Unit_BU__c / "
    "Department_ID_DID__c / Product2.Name (the .Name is used for the slingshot/databolt "
    "override below, replacing InvoiceLine.Name entirely, per Dakota: \"we shouldn't need "
    "invoice line name anymore for the name check\"). Product2 is the SOLE bu/did source "
    "now — there is no InvoiceLine fallback (an earlier version of this pipeline had one; "
    "removed per Dakota: \"it's okay to remove the invoice line resolution,\" since "
    "\"Product2 should not have a null or blank did/bu\").\n\n"
    "2. Defaults/overrides (apply_did_overrides()), in priority order:\n"
    "   - bu defaults to \"10901\" if still null (expected to rarely fire now).\n"
    "   - did overridden to \"16637\" if Product2.Name contains \"slingshot\", or "
    "\"16635\" if it contains \"databolt\" (databolt checked second, so it wins if a name "
    "somehow matches both).\n"
    "   - did overridden to \"16605\" (HIGHEST priority, overrides everything above) if "
    "gl_accounting_number_c = \"10040049\"."
)


def build_txn_type_sheet(wb):
    if "TransactionType to BU-DID" in wb.sheetnames:
        del wb["TransactionType to BU-DID"]
    ws = wb.create_sheet("TransactionType to BU-DID")
    for i, w in enumerate(T1_WIDTHS):
        ws.column_dimensions[chr(ord("A") + i)].width = w

    row = 1
    ws.merge_cells(f"A{row}:D{row}")
    c = ws.cell(row=row, column=1, value="TransactionType -> Product2 -> bu/did")
    c.font = TITLE_FONT
    row += 1
    ws.merge_cells(f"A{row}:D{row}")
    c = ws.cell(row=row, column=1,
                value="Reflects resolve_product2_id() + resolve_product2_fields() + "
                      "apply_product2_bu_did() + apply_did_overrides() in gl_source_join.py, "
                      "as of this date. All 15 confirmed TransactionType values covered.")
    c.font = NOTE_FONT
    row += 2

    for col, name in enumerate(T1_COLS, start=1):
        c = ws.cell(row=row, column=col, value=name)
        c.font = HEADER_FONT
        c.fill = HEADER_FILL
        c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        c.border = BORDER
    row += 1

    for txn_type, p2_path, evidence, status in TXN_TYPE_ROWS:
        status_key = "OPEN" if status.startswith("OPEN") else "CONFIRMED"
        values = [txn_type, p2_path, evidence, status]
        for col, val in enumerate(values, start=1):
            c = ws.cell(row=row, column=col, value=val)
            c.font = BODY_FONT
            c.border = BORDER
            c.alignment = Alignment(vertical="top", wrap_text=(col in (2, 3)))
            if col == 4:
                c.fill = STATUS_FILL[status_key]
                c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        row += 2  # extra row height room for wrapped text

    row += 1
    ws.merge_cells(f"A{row}:D{row}")
    c = ws.cell(row=row, column=1, value="Applies to every row above, in this order:")
    c.font = SECTION_FONT
    c.fill = SECTION_FILL
    for col in range(1, 5):
        ws.cell(row=row, column=col).fill = SECTION_FILL
    row += 1
    ws.merge_cells(f"A{row}:D{row+7}")
    c = ws.cell(row=row, column=1, value=TXN_TYPE_PRIORITY_NOTE)
    c.font = BODY_FONT
    c.alignment = Alignment(vertical="top", wrap_text=True)
    c.border = BORDER

    ws.freeze_panes = "A6"


# Each row: (field_no, field_name, position, length, source_value, intermediary_joins, status, notes)
N_A = "N/A — no join, standard/hardcoded value"

FILE_HEADER = [
    ("1", "Record Type", "1-2", "2", '"#H" (hardcoded)', N_A, "STANDARD", "file_header() — matches spec exactly."),
    ("2", "Creation Date", "3-10", "8", "creation_dt.strftime(\"%Y%m%d\")", N_A, "CONFIRMED", "creation_dt = when the file build ran, not any transaction's date."),
    ("3", "Creation Time", "11-16", "6", "creation_dt.strftime(\"%H%M%S\")", N_A, "CONFIRMED", None),
    ("4", "Transmit ID", "17-24", "8", "blank (param never passed a value)", N_A, "STANDARD", "file_header()'s transmit_id param exists but build_gl_file() never supplies one — matches spec's \"Fill with Spaces.\""),
    ("5", "Filler", "25-100", "76", "blank", N_A, "STANDARD", None),
]

JOURNAL_HEADER = [
    ("1", "Record Type", "1-1", "1", '"H" (hardcoded)', N_A, "STANDARD", None),
    ("2", "Business Unit", "2-6", "5", "Product2.Business_Unit_BU__c — the SOLE source now, defaulted to \"10901\" if it doesn't resolve", "See 'TransactionType to BU-DID' sheet for full join paths per TransactionType", "CONFIRMED", "Product2 is now the sole source, per Dakota — \"it's okay to remove the invoice line resolution\" (an earlier version fell back to InvoiceLine's own value; that layer is gone). Default applied in gl_source_join.apply_did_overrides()."),
    ("3", "Journal ID", "7-16", "10", '"NEXT" (hardcoded)', N_A, "STANDARD", "Matches spec exactly — G1 Load process assigns the real ID."),
    ("4", "Journal Date", "17-24", "8", "TransactionJournal.ActivityDate for this (BU, date) group, format MMDDYYYY", "TransactionJournal.ActivityDate directly — no join, just grouped by (business_unit, date)", "CONFIRMED", "One journal_header per (business_unit, activity_date) pair, not per BU alone — see build_gl_file()."),
    ("5", "Adjusting entry info", "25-28", "4", "blank", N_A, "STANDARD", None),
    ("6", "Average Daily Balance Date", "29-36", "8", "blank", N_A, "STANDARD", "Spec: load process defaults this to Journal Date."),
    ("7", "Ledger Group", "37-46", "10", '"RECORDING" (hardcoded)', N_A, "STANDARD", None),
    ("9", "Reversal Info", "47-67", "21", "blank", N_A, "STANDARD", "Field 8 is skipped in the original spec numbering — reproduced as-is, not a typo introduced here."),
    ("10", "Source", "68-70", "3", 'source param, defaults to "CS1"', "N/A — config value passed through run()/main()", "STANDARD", "Not derived from Salesforce data."),
    ("11", "Transaction Reference number", "71-78", "8", "blank", N_A, "STANDARD", None),
    ("12", "Description", "79-108", "30", '"RevCloud Batch" (hardcoded)', N_A, "OPEN", "Placeholder per Dakota — open question for what this should be when a BU/date batch spans multiple TransactionJournal.Name values."),
    ("13", "Default Currency Info", "109-141", "33", "blank", N_A, "STANDARD", None),
    ("14", "Filler", "142-180", "39", "blank", N_A, "STANDARD", None),
]

JOURNAL_LINE = [
    ("1", "Record Type", "1-1", "1", '"L" (hardcoded)', N_A, "STANDARD", None),
    ("2", "Business Unit", "2-6", "5", "same resolved value as Journal Header field 2", "See 'TransactionType to BU-DID' sheet", "CONFIRMED", None),
    ("3", "Journal Line Number", "7-15", "9", '"0" (hardcoded)', N_A, "STANDARD", "Matches spec — G1 Load process increments line numbers."),
    ("4", "Ledger", "16-25", "10", '"CORP" (always — hardcoded)', N_A, "OPEN", "Spec requires US BU -> CORP / Non-US BU -> LOCAL. Real business units are numeric (\"10901\"), not \"US\"/\"EU\"-prefixed, so no rule currently distinguishes them — pending a real rule per Dakota."),
    ("5", "Account", "26-35", "10", "GeneralLedgerAccount.GL_Accounting_Number__c", "TransactionJournal.Debit/CreditGeneralLedgerAccountId -> GeneralLedgerAccount.Id (direct join off TransactionJournal, unrelated to the bu/did chain)", "CONFIRMED", "Replaces the old Account.AccountNumber source (removed)."),
    ("6", "Alternate Account", "36-45", "10", "blank", N_A, "STANDARD", "Spec: not currently used by any Capital One BU."),
    ("7", "Department ID", "46-55", "10", "Product2.Department_ID_DID__c — the SOLE source now, with overrides", "See 'TransactionType to BU-DID' sheet for full join paths per TransactionType", "CONFIRMED", 'CreditMemoLine/DebitMemoLine can reach Product2 directly via their own Product2Id. Overridden to 16637/16635 if Product2.Name contains "slingshot"/"databolt" (moved from InvoiceLine.Name, per Dakota); overridden to 16605 (highest priority) if GL_Accounting_Number__c = "10040049".'),
    ("8", "Unused Chartfields", "56-92", "37", "blank", N_A, "STANDARD", None),
    ("9", "Affiliate", "93-97", "5", "blank (not populated)", N_A, "STANDARD", "Spec marks this optional — no source currently mapped."),
    ("10", "Unused Chartfields", "98-127", "30", "blank", N_A, "STANDARD", None),
    ("11", "Reg Code", "128-137", "10", "blank", N_A, "STANDARD", "Spec: not used on the RECORDING ledger."),
    ("12", "Unused Chartfields", "138-147", "10", "blank", N_A, "STANDARD", None),
    ("13", "Project ID", "148-162", "15", "blank (not populated)", "N/A — gap, no source or join mapped at all yet", "OPEN", "Spec marks this optional, but no source field is mapped at all yet — gap, not a confirmed \"intentionally blank.\""),
    ("14", "Filler", "163-187", "25", "blank", N_A, "STANDARD", None),
    ("15", "Base Currency Amount", "188-215", "28", '"0" (hardcoded)', N_A, "STANDARD", None),
    ("16", "Movement Flag", "216-216", "1", "blank", N_A, "STANDARD", None),
    ("17", "Statistics amount", "217-233", "17", '"0" (hardcoded)', N_A, "STANDARD", None),
    ("18", "Journal Line reference", "234-243", "10", "TransactionJournal.Name", "TransactionJournal.Name directly — no join", "CONFIRMED", "Previously TransactionJournal.UsageType (marked \"?\" — tentative); corrected per Dakota."),
    ("19", "Journal Line Description", "244-273", "30", "TransactionJournal.TransactionType", "TransactionJournal.TransactionType directly — no join", "CONFIRMED", "Previously marked \"?\" alongside field 18 above — this one was already correct; a generic type label (\"InvoiceLine\"/\"Payment\"/etc.), not a free-text description."),
    ("20", "Transaction Currency Code", "274-276", "3", "blank (not populated)", "N/A — gap, no source or join mapped", "OPEN", "No source field currently mapped — gap."),
    ("21", "Currency Rate Type", "277-281", "5", '"USDLY" (hardcoded)', N_A, "STANDARD", None),
    ("22", "Transaction Monetary Amount", "282-309", "28", "TransactionJournal.CreditDebit", "TransactionJournal.Credit / .Debit directly — no join, sign-derived (resolve_amount())", "CONFIRMED", "Derived: -Credit if Credit is populated, else Debit (Credit takes priority if both populated)."),
    ("23", "Currency Exchange Rate", "310-326", "17", '"0" (hardcoded)', N_A, "STANDARD", None),
    ("24", "Filler", "327-418", "92", "blank", N_A, "STANDARD", None),
]

FILE_TRAILER = [
    ("1", "Record Type", "1-2", "2", '"#T" (hardcoded)', N_A, "STANDARD", None),
    ("2", "Row Count", "3-11", "9", "count of Journal Header + Journal Line records written", "Computed from the assembled record count — not a database join", "CONFIRMED", None),
    ("3", "Total Debits", "12-39", "28", "sum of positive Transaction Monetary Amounts", "Computed by summing across all journal lines — not a database join", "CONFIRMED", None),
    ("4", "Total Credits", "40-67", "25", "sum of negative Transaction Monetary Amounts", "Computed by summing across all journal lines — not a database join", "CONFIRMED", None),
    ("5", "Total Statistical Amount", "68-95", "25", '"0" (hardcoded)', N_A, "STANDARD", "Consistent with Statistics Amount (Journal Line field 17) always being \"0\" too — nothing currently sources either one from real data."),
    ("6", "Filler", "96-100", "5", "blank", N_A, "STANDARD", None),
]

SECTIONS = [
    ("File Header", FILE_HEADER),
    ("Journal Header Rows", JOURNAL_HEADER),
    ("Journal Line Rows", JOURNAL_LINE),
    ("File Trailer", FILE_TRAILER),
]


def build():
    wb = openpyxl.load_workbook(SRC)
    if "Mapping - Source to File" in wb.sheetnames:
        del wb["Mapping - Source to File"]
    ws = wb.create_sheet("Mapping - Source to File")
    # Place right after the original sheet for easy side-by-side reference.
    wb.move_sheet("Mapping - Source to File", offset=-(len(wb.sheetnames) - 2))

    for i, w in enumerate(COL_WIDTHS):
        ws.column_dimensions[chr(ord("A") + i)].width = w

    row = 1
    ws.merge_cells(f"A{row}:H{row}")
    c = ws.cell(row=row, column=1, value="Global One General Ledger")
    c.font = TITLE_FONT
    row += 1
    ws.merge_cells(f"A{row}:H{row}")
    c = ws.cell(row=row, column=1, value="Journal Entry Interface — Source Field Mapping")
    c.font = Font(name=FONT_NAME, size=12, bold=True)
    row += 1
    ws.merge_cells(f"A{row}:H{row}")
    c = ws.cell(row=row, column=1,
                value="Reflects helpers/gl_source_join.py + salesforce_global_one.py as of this date. "
                      "See legend below for status colors.")
    c.font = NOTE_FONT
    row += 2

    # Legend
    legend_start = row
    ws.cell(row=row, column=1, value="Legend:").font = SECTION_FONT
    row += 1
    legend = [
        ("CONFIRMED", "Directly sourced from a real Salesforce field, confirmed against the pipeline code."),
        ("STANDARD", "A hardcoded standard value or intentional blank per the file spec — no source mapping needed."),
        ("TENTATIVE", "Mapped, but marked uncertain (\"?\") in earlier design notes — not fully confirmed."),
        ("OPEN", "Placeholder, open decision, or gap — no confirmed real value/rule yet."),
    ]
    for label, desc in legend:
        ws.cell(row=row, column=1, value=label).fill = STATUS_FILL[label]
        ws.cell(row=row, column=1).font = Font(name=FONT_NAME, size=9, bold=True)
        ws.cell(row=row, column=1).alignment = Alignment(horizontal="center")
        ws.merge_cells(f"B{row}:H{row}")
        ws.cell(row=row, column=2, value=desc).font = NOTE_FONT
        row += 1
    row += 1

    for section_name, rows in SECTIONS:
        ws.merge_cells(f"A{row}:H{row}")
        c = ws.cell(row=row, column=1, value=section_name)
        c.font = SECTION_FONT
        c.fill = SECTION_FILL
        for col in range(1, 9):
            ws.cell(row=row, column=col).fill = SECTION_FILL
        row += 1

        for col, name in enumerate(COLS, start=1):
            c = ws.cell(row=row, column=col, value=name)
            c.font = HEADER_FONT
            c.fill = HEADER_FILL
            c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            c.border = BORDER
        row += 1

        for field_no, field_name, position, length, source_value, intermediary_joins, status, notes in rows:
            values = [field_no, field_name, position, length, source_value, intermediary_joins, status, notes or ""]
            for col, val in enumerate(values, start=1):
                c = ws.cell(row=row, column=col, value=val)
                c.font = BODY_FONT
                c.border = BORDER
                c.alignment = Alignment(vertical="top", wrap_text=(col in (5, 6, 8)))
                if col == 7:
                    c.fill = STATUS_FILL[status]
                    c.alignment = Alignment(horizontal="center", vertical="center")
            row += 1
        row += 1  # blank row between sections

    ws.freeze_panes = "A1"

    build_txn_type_sheet(wb)
    # Place right after the Mapping sheet for easy cross-reference.
    wb.move_sheet("TransactionType to BU-DID", offset=-(len(wb.sheetnames) - 3))

    wb.save(OUT)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    build()
