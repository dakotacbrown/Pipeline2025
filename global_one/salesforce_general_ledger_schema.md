# Salesforce General Ledger — Schema Reference

Transcribed directly from Dakota's "Salesforce General Ledger" Google Sheet
(all tabs shared to date). Kept here as a running reference for
`gl_source_join.py` / `salesforce_global_one.py` development — not
independently re-verified against the live Salesforce org, so still worth
a spot-check if precision matters for something high-stakes. Supersedes
the earlier partial `CreditMemoLineInvoiceLine`-only transcription (which
also had `Name`'s Column Name/Label backwards — corrected below).

Tables are listed alphabetically by object (tab) name, matching the
sheet's own ordering.

---

## Fields relevant to `resolve_product2_id()`'s resolution paths

Quick index — the fields that actually matter for bu/did resolution,
pulled out of the full tables below for fast reference:

| Object | Field | Purpose |
|---|---|---|
| **Product2** | `Business_Unit_BU__c`, `Department_ID_DID__c`, `Name` | The SOLE bu/did source, universally, per Dakota. `.Name` is used for the slingshot/databolt did override. |
| InvoiceLine | `Product2Id` | Join key reaching Product2 for most TransactionTypes |
| CreditMemoLine | `Product2Id` | Direct join key for CreditMemo-family TransactionTypes — bypasses InvoiceLine entirely |
| DebitMemoLine | `Product2Id` | Direct join key for DebitMemoLine/DebitMemoLineTax |

**RESOLVED, per Dakota:** Product2 is the SOLE bu/did source, universally
— "Product2 should not have a null or blank did/bu... it's okay to
remove the invoice line resolution." InvoiceLine's own
`Business_Unit_BU__c`/`Department_ID_DID__c` fields are NOT read for
bu/did purposes at all anymore — an earlier version of this pipeline used
InvoiceLine as a fallback; that layer is gone. `InvoiceLine.Name` is also
no longer read for the slingshot/databolt check — `Product2.Name`
replaced it entirely, per Dakota: "we shouldn't need invoice line name
anymore for the name check."

## TransactionType picklist — full coverage status

The `TransactionType` field's full picklist has **15 confirmed values**
(per Dakota's screenshots of the field's `picklistValues`), transcribed
here in the sheet's own order. **All 15 are now handled** — the earlier
"7 handled / 7 not yet handled" gap documented in a previous version of
this note has been fully closed:

| # | Value | Coverage in `resolve_product2_id()` |
|---|---|---|
| 1 | `Invoice` | CONFIRMED — via `InvoiceLine.InvoiceId`, no separate Invoice table load needed |
| 2 | `InvoiceLine` | CONFIRMED — direct |
| 3 | `InvoiceLineTax` | CONFIRMED — one hop via `InvoiceLine` |
| 4 | `CreditMemo` | CONFIRMED — 2 paths: direct via `CreditMemoLine.Product2Id`, or header fallback via `CreditMemoInvApplication` (see caveat below — this used to be 3 paths) |
| 5 | `CreditMemoLine` | CONFIRMED — direct entry, its own standalone TransactionType |
| 6 | `CreditMemoLineTax` | CONFIRMED — via `CreditMemoLine` |
| 7 | `Payment` | CONFIRMED — via header path (864 rows); does NOT require a matching row in the `Payment` table itself (a real bug, since fixed — see caveat below) |
| 8 | `Refund` | CONFIRMED — structurally complete, blocked on `refund_line_payment`'s dataset_id (still a PLACEHOLDER) |
| 9 | `PaymentLineInvoice` | CONFIRMED — direct entry via `InvoiceLine` |
| 10 | `CreditMemoInvApplication` | CONFIRMED — direct entry via `CreditMemoLine` |
| 11 | `CreditMemoLineInvoiceLine` | CONFIRMED — direct entry via `CreditMemoLine` |
| 12 | `RefundLinePayment` | CONFIRMED — structurally complete, same PLACEHOLDER block as `Refund` |
| 13 | `DebitMemoLine` | CONFIRMED — direct |
| 14 | `DebitMemoLineTax` | CONFIRMED (structurally) — no corresponding table exists in Salesforce YET, per Dakota; built anyway ("I'll still need to have the mapping in case it goes live") |

**Caveat 1 — CreditMemo's "line-level via CreditMemoLineInvoiceLine" path
was removed, confirmed structurally DEAD:** it derived from the same
`CreditMemoLine` table, keyed by the same `CreditMemoId`, as the direct
candidate, which is always tried first — since the dedup keeps the first
candidate regardless of whether its value is null, that junction-table
path could never actually change the result, even in the one case where
it would have returned a better (non-null) answer. This was a real
candidate before the Product2 rewrite (it existed to reach InvoiceLine's
own bu/did, which the pre-rewrite "direct" candidate couldn't do) — the
rewrite made it redundant without this being caught until later testing.

**Caveat 2 — `Payment`'s own resolution is deliberately independent of
the `Payment` table:** confirmed via a real end-to-end run against
fixture data that gating `TransactionType="Payment"`'s own resolution on
a matching row in the separately-loaded `Payment` table was a bug (a
regression, since fixed) — `ReferenceTransactionRecordId` already IS the
PaymentId directly for this type;`payment_line_invoice_line`/
`payment_line_invoice` are the real source of truth for which PaymentIds
exist. The `Payment` table IS still explicitly joined for
`RefundLinePayment`/`Refund` specifically, per Dakota: "keep payment just
to make sure there's nothing lost in the joins" — that confirmation hop
was always meant for those two types, not for `Payment`'s own resolution.

---

## CreditMemo

| Column Name | Column Label | Datatype | Nullable | Data Length | Notes |
|---|---|---|---|---|---|
| Id | Credit Memo ID | varchar | FALSE | 18 | |
| OwnerId | Owner ID | varchar | FALSE | 18 | |
| IsDeleted | Deleted | bool | FALSE | | |
| DocumentNumber | Document Number | varchar | FALSE | 255 | |
| CreatedDate | Created Date | timestamp_tz | FALSE | | |
| CreatedById | Created By ID | varchar | FALSE | 18 | |
| LastModifiedDate | Last Modified Date | timestamp_tz | FALSE | | |
| LastModifiedById | Last Modified By ID | varchar | FALSE | 18 | |
| SystemModstamp | System Modstamp | timestamp_tz | FALSE | | |
| LastViewedDate | Last Viewed Date | timestamp_tz | TRUE | | |
| LastReferencedDate | Last Referenced Date | timestamp_tz | TRUE | | |
| BillingAccountId | Account ID | varchar | FALSE | 18 | |
| ReferenceEntityId | ReferenceEntity ID | varchar | TRUE | 18 | |
| CreditMemoNumber | Credit Memo Number | varchar | TRUE | 255 | |
| TotalAmount | Total Amount | double | TRUE | precision 18 scale 2 | |
| TotalAmountWithTax | Total with Tax | double | TRUE | precision 18 scale 2 | |
| TotalChargeAmount | Total Charges | double | TRUE | precision 18 scale 2 | |
| TotalAdjustmentAmount | Total Adjustment Amount | double | TRUE | precision 18 scale 2 | |
| TotalTaxAmount | Total Tax | double | TRUE | precision 18 scale 2 | |
| CreditDate | Credit Date | date | TRUE | | |
| Description | Description | varchar | TRUE | 255 | |
| Status | Status | varchar | FALSE | 255 | |
| BillToContactId | Contact ID | varchar | TRUE | 18 | |
| Balance | Balance | double | TRUE | precision 18 scale 2 | |
| SettlementLevel | Settlement Level | varchar | TRUE | 255 | |
| EffectiveDate | Effective Date | date | TRUE | | |
| NetCreditsApplied | Net Credits Applied | double | TRUE | precision 18 scale 2 | |
| CreationMode | Creation Mode | varchar | TRUE | 255 | |
| ExternalReference | External Reference | varchar | TRUE | 255 | |
| ExternalReferenceDataSource | External Reference Data Source | varchar | TRUE | 255 | |
| SourceAction | Source Action | varchar | TRUE | 255 | |
| Category | Category | varchar | TRUE | 255 | |
| ReasonCode | Reason Code | varchar | TRUE | 40 | |
| LegalEntityId | Legal Entity ID | varchar | TRUE | 18 | |
| LegalEntityAccountingPeriodId | Legal Entity Accounting Period ID | varchar | TRUE | 18 | |
| Zuora_Id__c | Zuora Id | varchar | TRUE | 255 | |

---

## CreditMemoLine

| Column Name | Column Label | Datatype | Nullable | Data Length | Notes |
|---|---|---|---|---|---|
| Id | Credit Memo Line ID | varchar | FALSE | 18 | |
| IsDeleted | Deleted | bool | FALSE | | |
| Name | Name | varchar | FALSE | 255 | |
| CreatedDate | Created Date | timestamp_tz | FALSE | | |
| CreatedById | Created By ID | varchar | FALSE | 18 | |
| LastModifiedDate | Last Modified Date | timestamp_tz | FALSE | | |
| LastModifiedById | Last Modified By ID | varchar | FALSE | 18 | |
| SystemModstamp | System Modstamp | timestamp_tz | FALSE | | |
| CreditMemoId | Credit Memo ID | varchar | FALSE | 18 | |
| ReferenceEntityItemId | ReferenceEntityItem ID | varchar | TRUE | 18 | |
| StartDate | Start Date | date | TRUE | | |
| EndDate | End Date | date | TRUE | | |
| TaxEffectiveDate | Tax Effective Date | date | TRUE | | |
| Status | Status | varchar | TRUE | 50 | |
| ChargeAmount | Charge Amount | double | TRUE | precision 18 scale 2 | |
| TaxAmount | Tax Amount | double | TRUE | precision 18 scale 2 | |
| AdjustmentAmount | Adjustment Amount | double | TRUE | precision 18 scale 2 | |
| LineAmount | Line Amount | double | TRUE | precision 18 scale 2 | |
| Description | Description | varchar | TRUE | 255 | |
| ReferenceEntityItemTypeCode | Reference Entity Item Type Code | varchar | TRUE | 255 | |
| ReferenceEntityItemType | Reference Entity Item Type | varchar | TRUE | 40 | |
| **Product2Id** | Product ID | varchar | TRUE | 18 | Fallback join key to Product2 |
| TaxTreatmentId | Tax Treatment ID | varchar | TRUE | 18 | |
| ShippingAddressId | Credit Memo Address Group ID | varchar | TRUE | 18 | |
| BillingAddressId | Credit Memo Address Group ID | varchar | TRUE | 18 | |
| LegalEntityId | Legal Entity ID | varchar | TRUE | 18 | |
| LegalEntityAccountingPeriodId | Legal Entity Accounting Period ID | varchar | TRUE | 18 | |
| Balance | Balance | double | TRUE | precision 18 scale 2 | |
| NetCreditsApplied | Net Credits Applied | double | TRUE | precision 18 scale 2 | |
| ShipFromAddressId | Credit Memo Address Group ID | varchar | TRUE | 18 | |

---

## CreditMemoLineInvApplication

| Column Name | Column Label | Datatype | Nullable | Data Length | Notes |
|---|---|---|---|---|---|
| Id | Credit Memo Invoice Application ID | varchar | FALSE | 18 | |
| IsDeleted | Deleted | bool | FALSE | | |
| CreditMemoInvoiceNumber | Name | varchar | FALSE | 255 | |
| CreatedDate | Created Date | timestamp | FALSE | | |
| CreatedById | Created By ID | varchar | FALSE | 18 | |
| LastModifiedDate | Last Modified Date | timestamp | FALSE | | |
| LastModifiedById | Last Modified By ID | varchar | FALSE | 18 | |
| SystemModstamp | System Modstamp | timestamp | FALSE | | |
| InvoiceId | Invoice ID | varchar | FALSE | 18 | |
| CreditMemoId | Credit Memo ID | varchar | FALSE | 18 | |
| Amount | Amount | double | FALSE | precision 18 scale 2 | |
| Type | Type | varchar | FALSE | 255 | |
| Description | Description | varchar | TRUE | 255 | |
| Date | Date | timestamp | TRUE | | |
| AppliedDate | Applied Date | timestamp | TRUE | | |
| EffectiveDate | Effective Date | timestamp | TRUE | | |
| UnappliedDate | Unapplied Date | timestamp | TRUE | | |
| AssociatedLineId | Credit Memo Invoice Application ID | varchar | TRUE | 18 | |
| HasBeenUnapplied | Has Been Unapplied | varchar | TRUE | 255 | |
| CreditMemoBalance | Credit Memo Balance | double | TRUE | precision 18 scale 2 | |
| InvoiceBalance | Invoice Balance | double | TRUE | precision 18 scale 2 | |
| ImpactAmount | Impact Amount | double | TRUE | precision 18 scale 2 | |

---

## CreditMemoLineInvoiceLine

| Column Name | Column Label | Datatype | Nullable | Data Length | Notes |
|---|---|---|---|---|---|
| Id | Credit Memo Line Invoice Line ID | varchar | FALSE | 18 | |
| IsDeleted | Deleted | bool | FALSE | | |
| CreditMemoLineInvoiceLineNumber | Name | varchar | FALSE | 255 | corrected — Column Name/Label were swapped in an earlier version of this doc |
| CreatedDate | Created Date | timestamp | FALSE | | |
| CreatedById | Created By ID | varchar | FALSE | 18 | |
| LastModifiedDate | Last Modified Date | timestamp | FALSE | | |
| LastModifiedById | Last Modified By ID | varchar | FALSE | 18 | |
| SystemModstamp | System Modstamp | timestamp | FALSE | | |
| **InvoiceLineId** | Invoice Line ID | varchar | FALSE | 18 | |
| **CreditMemoLineId** | Credit Memo Line ID | varchar | FALSE | 18 | |
| Amount | Amount | double | FALSE | precision 18 scale 2 | |
| Type | Type | varchar | FALSE | 255 | |
| Description | Description | varchar | TRUE | 255 | |
| AppliedDateTime | Applied Date Time | timestamp | TRUE | | |
| EffectiveDateTime | Effective Date Time | timestamp | TRUE | | |
| UnappliedDateTime | Unapplied Date Time | timestamp | TRUE | | |
| RelatedCrMemoLineInvcLineId | Credit Memo Line Invoice Line ID | varchar | TRUE | 18 | |
| UnappliedStatus | Unapplied Status | varchar | FALSE | 255 | |
| CreditMemoLineBalance | Credit Memo Line Balance | double | TRUE | precision 18 scale 2 | |
| InvoiceLineBalance | Invoice Line Balance | double | TRUE | precision 18 scale 2 | |
| ImpactAmount | Impact Amount | double | TRUE | precision 18 scale 2 | |
| LegalEntityId | Legal Entity ID | varchar | TRUE | 18 | |
| LegalEntityAccountingPeriodId | Legal Entity Accounting Period ID | varchar | TRUE | 18 | |

`InvoiceLineId`/`CreditMemoLineId` both `nullable: FALSE` — any row that
exists is guaranteed joinable on both ends. Matches
`EXPECTED_COLUMNS["credit_memo_line_invoice_line"]` exactly.

---

## CreditMemoLineTax

| Column Name | Column Label | Datatype | Nullable | Data Length | Notes |
|---|---|---|---|---|---|
| Id | Credit Memo Line Tax ID | varchar | FALSE | 18 | |
| IsDeleted | Deleted | bool | FALSE | | |
| CreditMemoLineTaxNumber | Name | varchar | FALSE | 255 | |
| CreatedDate | Created Date | timestamp | FALSE | | |
| CreatedById | Created By ID | varchar | FALSE | 18 | |
| LastModifiedDate | Last Modified Date | timestamp | FALSE | | |
| LastModifiedById | Last Modified By ID | varchar | FALSE | 18 | |
| SystemModstamp | System Modstamp | timestamp | FALSE | | |
| CreditMemoLineId | Credit Memo Line ID | varchar | FALSE | 18 | |
| TaxAmount | Tax Amount | double | TRUE | precision 18 scale 2 | |
| Description | Description | varchar | TRUE | 255 | |
| StartDate | Start Date | date | TRUE | | |
| EndDate | End Date | date | TRUE | | |
| TaxName | Tax Name | varchar | TRUE | 255 | |
| TaxCode | Tax Code | varchar | TRUE | 255 | |
| TaxRate | Tax Rate | double | TRUE | precision 5 scale 2 | |
| TaxTransactionNumber | Tax Transaction Number | varchar | TRUE | 255 | |
| TaxDocumentNumber | Tax Document Number | varchar | TRUE | 255 | |
| TaxEffectiveDate | Tax Effective Date | date | TRUE | | |
| TaxTreatmentId | Tax Treatment ID | varchar | TRUE | 18 | |
| ShippingAddressId | Credit Memo Address Group ID | varchar | TRUE | 18 | |
| BillingAddressId | Credit Memo Address Group ID | varchar | TRUE | 18 | |
| CalculationStatus | Calculation Status | varchar | TRUE | 255 | |
| LegalEntityId | Legal Entity ID | varchar | TRUE | 18 | |
| LegalEntityAccountingPeriodId | Legal Entity Accounting Period ID | varchar | TRUE | 18 | |
| ReferenceEntityItemId | Invoice Line Tax ID | varchar | TRUE | 18 | |
| ShipFromAddressId | Credit Memo Address Group ID | varchar | TRUE | 18 | |

*(Not currently used by `gl_source_join.py` — not loaded by `build_source_dataframe()`.)*

---

## DebitMemoLine

| Column Name | Column Label | Datatype | Nullable | Data Length | Notes |
|---|---|---|---|---|---|
| Id | Debit Memo Line ID | varchar | FALSE | 18 | |
| IsDeleted | Deleted | bool | FALSE | | |
| Name | Name | varchar | FALSE | 255 | |
| CreatedDate | Created Date | timestamp_tz | FALSE | | |
| CreatedById | Created By ID | varchar | FALSE | 18 | |
| LastModifiedDate | Last Modified Date | timestamp_tz | FALSE | | |
| LastModifiedById | Last Modified By ID | varchar | FALSE | 18 | |
| SystemModstamp | System Modstamp | timestamp_tz | FALSE | | |
| LastViewedDate | Last Viewed Date | timestamp_tz | TRUE | | |
| LastReferencedDate | Last Referenced Date | timestamp_tz | TRUE | | |
| DebitMemoId | Debit Memo ID | varchar | FALSE | 18 | |
| **ReferenceRecordId** | Reference Record ID | varchar | TRUE | 18 | -> InvoiceLine.Id (confirmed by Dakota) |
| StartDate | Start Date | date | TRUE | | |
| EndDate | End Date | date | TRUE | | |
| ChargeAmount | Charge Amount | double | FALSE | precision 18 scale 2 | |
| Description | Description | varchar | TRUE | 255 | |
| Product2Id | Product ID | varchar | TRUE | 18 | Fallback join key to Product2 |
| TaxTreatmentId | Tax Treatment ID | varchar | TRUE | 18 | |
| ShippingAddressId | Debit Memo Address ID | varchar | TRUE | 18 | |
| BillingAddressId | Debit Memo Address ID | varchar | TRUE | 18 | |
| LegalEntityId | Legal Entity ID | varchar | TRUE | 18 | |
| LegalEntityAccountingPeriodId | Legal Entity Accounting Period ID | varchar | TRUE | 18 | |

`ReferenceRecordId` is `nullable: TRUE` — unlike `CreditMemoLineInvoiceLine`'s
join keys, a `DebitMemoLine` row is NOT guaranteed to have a matching
`InvoiceLine`. `resolve_product2_id()` already tolerates this correctly (a
left join, not an inner one) — though this specific hop is now moot for
`DebitMemoLine`'s own TransactionType anyway, since it reaches
`DebitMemoLine.Product2Id` directly without needing `InvoiceLine` at all.

---

## GeneralLedgerAccount

*Also seen under the sheet tab name `Template_2` — identical column set.*

| Column Name | Column Label | Datatype | Nullable | Data Length | Notes |
|---|---|---|---|---|---|
| Id | General Ledger Account ID | varchar | FALSE | 18 | |
| OwnerId | Owner ID | varchar | FALSE | 18 | |
| IsDeleted | Deleted | bool | FALSE | | |
| Name | Name | varchar | FALSE | 255 | |
| CreatedDate | Created Date | timestamp_tz | FALSE | | |
| CreatedById | Created By ID | varchar | FALSE | 18 | |
| LastModifiedDate | Last Modified Date | timestamp_tz | FALSE | | |
| LastModifiedById | Last Modified By ID | varchar | FALSE | 18 | |
| SystemModstamp | System Modstamp | timestamp_tz | FALSE | | |
| LastViewedDate | Last Viewed Date | timestamp_tz | TRUE | | |
| LastReferencedDate | Last Referenced Date | timestamp_tz | TRUE | | |
| AccountingCode | Accounting Code | varchar | FALSE | 40 | |
| AccountingName | Accounting Name | varchar | FALSE | 40 | |
| AccountingType | Accounting Type | varchar | TRUE | 40 | |
| FinancialStatement | Financial Statement | varchar | TRUE | 40 | |
| LegalEntityId | Legal Entity ID | varchar | TRUE | 18 | |
| Description | Description | varchar | TRUE | 255 | |
| Type | Type | varchar | FALSE | 255 | |
| **GL_Accounting_Number__c** | GL Accounting Number | varchar | TRUE | 255 | The journal line's Account field source |

---

## Invoice

| Column Name | Column Label | Datatype | Nullable | Data Length | Notes |
|---|---|---|---|---|---|
| Id | Invoice ID | varchar | FALSE | 18 | |
| OwnerId | Owner ID | varchar | FALSE | 18 | |
| IsDeleted | Deleted | bool | FALSE | | |
| DocumentNumber | Document Number | varchar | FALSE | 255 | |
| CreatedDate | Created Date | timestamp_tz | FALSE | | |
| CreatedById | Created By ID | varchar | FALSE | 18 | |
| LastModifiedDate | Last Modified Date | timestamp_tz | FALSE | | |
| LastModifiedById | Last Modified By ID | varchar | FALSE | 18 | |
| SystemModstamp | System Modstamp | timestamp_tz | FALSE | | |
| LastViewedDate | Last Viewed Date | timestamp_tz | TRUE | | |
| LastReferencedDate | Last Referenced Date | timestamp_tz | TRUE | | |
| ReferenceEntityId | ReferenceEntity ID | varchar | TRUE | 18 | |
| InvoiceNumber | Invoice Number | varchar | TRUE | 255 | |
| BillingAccountId | Account ID | varchar | FALSE | 18 | |
| TotalAmount | Total Amount | double | TRUE | precision 18 scale 2 | |
| TotalAmountWithTax | Total with Tax | double | TRUE | precision 18 scale 2 | |
| TotalChargeAmount | Total Charges | double | TRUE | precision 18 scale 2 | |
| TotalAdjustmentAmount | Total Adjustment Amount | double | TRUE | precision 18 scale 2 | |
| TotalTaxAmount | Total Tax | double | TRUE | precision 18 scale 2 | |
| Status | Status | varchar | FALSE | 255 | |
| InvoiceDate | Invoice Date | date | FALSE | | |
| DueDate | Due Date | date | FALSE | | |
| BillToContactId | Contact ID | varchar | TRUE | 18 | |
| Description | Description | varchar | TRUE | 255 | |
| Balance | Balance | double | TRUE | precision 18 scale 2 | |
| BalanceValue | Balance Value | double | TRUE | precision 18 scale 6 | |
| TotalChargeTaxAmount | Total Charge Tax Amount | double | TRUE | precision 18 scale 2 | |
| TotalChargeAmountWithTax | Total Charge Amount with Tax | double | TRUE | precision 18 scale 2 | |
| TotalAdjustmentTaxAmount | Total Adjustment Tax Amount | double | TRUE | precision 18 scale 2 | |
| TotalAdjustmentAmountWithTax | Total Adjustment Amount with Tax | double | TRUE | precision 18 scale 2 | |
| NetCreditsApplied | Net Credits Applied | double | TRUE | precision 18 scale 2 | |
| NetPaymentsApplied | Net Payments Applied | double | TRUE | precision 18 scale 2 | |
| CreationMode | Creation Mode | varchar | TRUE | 255 | |
| AppType | App Type | varchar | TRUE | 255 | |
| PaymentTermId | Payment Term ID | varchar | TRUE | 18 | |
| InvoiceBatchRunId | Invoice Batch Run ID | varchar | TRUE | 18 | |
| GroupingKey | Grouping Key | varchar | TRUE | 255 | |
| SettlementStatus | Settlement Status | varchar | TRUE | 255 | |
| FullSettlementDate | Full Settlement Date | date | TRUE | | |
| DaysInvoiceOverdue | Days Invoice Overdue | int | TRUE | 9 | |
| DaysInvoiceOpen | Days Invoice Open | int | TRUE | 9 | |
| TotalConvertedNegAmount | Total Converted Negative Amount | double | TRUE | precision 18 scale 2 | |
| InvBatchDraftToPostedRunId | Invoice Batch Draft to Posted Run ID | varchar | TRUE | 18 | |
| PostedDate | Posted Date | date | TRUE | | |
| SettlementLevel | Settlement Level | varchar | TRUE | 255 | |
| IsInvoiceLocked | Invoice Locked | bool | FALSE | | |
| InvoiceLockedDateTime | Invoice Locked Date Time | timestamp_tz | TRUE | | |
| InvoiceReference | Invoice Reference | varchar | TRUE | 255 | |
| SavedPaymentMethodId | Saved Payment Method ID | varchar | TRUE | 18 | |
| ShouldExcludePayment | Skip Payment Schedule Creation | bool | FALSE | | |
| PaymentExclusionReason | Skip Payment Schedule Creation Reason | varchar | TRUE | 40 | |
| IsBillingScheduleGroupSkipped | Is Billing Schedule Skipped | bool | FALSE | | |
| UniqueIdentifier | Unique Identifier | varchar | TRUE | 255 | |
| WriteOffStatus | Write Off Status | varchar | TRUE | 255 | |
| WriteOffTotalChargeAmount | Write Off Total Charge Amount | double | TRUE | precision 18 scale 2 | |
| WriteOffTotalTaxAmount | Write Off Total Tax Amount | double | TRUE | precision 18 scale 2 | |
| LegalEntityId | Legal Entity ID | varchar | TRUE | 18 | |
| SequencePolicyId | Sequence Policy ID | varchar | TRUE | 18 | |
| LegalEntityAccountingPeriodId | Legal Entity Accounting Period ID | varchar | TRUE | 18 | |
| LastEmailDispatchStatus | Last Email Dispatch Status | varchar | TRUE | 255 | |
| Due_Date_Age__c | Due Date Age | double | TRUE | precision 18 scale 0 | |
| Order__c | Order | varchar | TRUE | 18 | |
| PO_Number__c | PO Number | varchar | TRUE | 255 | |
| Past_Due_Balance__c | Past Due Balance | double | TRUE | precision 18 scale 2 | |
| Zuora_Id__c | Zuora Id | varchar | TRUE | 255 | |

*(Not loaded by `build_source_dataframe()` — resolution goes straight from PaymentLineInvoice/CreditMemoLineInvApplication's own InvoiceId to InvoiceLine.InvoiceId, without an Invoice hop.)*

---

## InvoiceLine

| Column Name | Column Label | Datatype | Nullable | Data Length | Notes |
|---|---|---|---|---|---|
| Id | Invoice Line ID | varchar | FALSE | 18 | |
| IsDeleted | Deleted | bool | FALSE | | |
| Name | Name | varchar | FALSE | 255 | |
| CreatedDate | Created Date | timestamp_tz | FALSE | | |
| CreatedById | Created By ID | varchar | FALSE | 18 | |
| LastModifiedDate | Last Modified Date | timestamp_tz | FALSE | | |
| LastModifiedById | Last Modified By ID | varchar | FALSE | 18 | |
| SystemModstamp | System Modstamp | timestamp_tz | FALSE | | |
| InvoiceId | Invoice ID | varchar | FALSE | 18 | |
| BillingScheduleId | Billing Schedule ID | varchar | TRUE | 18 | |
| BillingScheduleGroupId | Billing Schedule Group ID | varchar | TRUE | 18 | |
| HasMultipleItems | Has Multiple Items | bool | FALSE | | |
| IsUsageBasedInvoiceLine | Usage Based Invoice Line | bool | FALSE | | |
| UsageOverageQuantity | Usage Overage Quantity | double | TRUE | precision 18 scale 2 | |
| ReferenceEntityItemId | ReferenceEntityItem ID | varchar | TRUE | 18 | |
| GroupReferenceEntityItemId | GroupReferenceEntityItem ID | varchar | TRUE | 18 | |
| LineAmount | Line Amount | double | TRUE | precision 18 scale 2 | |
| Quantity | Quantity | double | TRUE | precision 18 scale 2 | |
| UnitPrice | Unit Price | double | TRUE | precision 18 scale 2 | |
| ChargeAmount | Charge Amount | double | TRUE | precision 18 scale 2 | |
| TaxAmount | Tax Amount | double | TRUE | precision 18 scale 2 | |
| AdjustmentAmount | Adjustment Amount | double | TRUE | precision 18 scale 2 | |
| Balance | Balance | double | TRUE | precision 18 scale 2 | |
| NetPaymentsApplied | Net Payments Applied | double | TRUE | precision 18 scale 2 | |
| NetCreditsApplied | Net Credits Applied | double | TRUE | precision 18 scale 2 | |
| InvoiceStatus | Status | varchar | TRUE | 50 | |
| Description | Description | varchar | TRUE | 255 | |
| InvoiceLineStartDate | Invoice Line Start Date | date | FALSE | | |
| InvoiceLineEndDate | Invoice Line End Date | date | FALSE | | |
| ReferenceEntityItemType | Reference Entity Item Type | varchar | TRUE | 40 | |
| ReferenceEntityItemTypeCode | Reference Entity Item Type Code | varchar | TRUE | 255 | |
| **Product2Id** | Product ID | varchar | TRUE | 18 | Fallback join key to Product2 |
| Type | Type | varchar | FALSE | 255 | |
| ChargeTaxAmount | Charge Tax Amount | double | TRUE | precision 18 scale 2 | |
| ChargeAmountWithTax | Charge Amount with Tax | double | TRUE | precision 18 scale 2 | |
| AdjustmentTaxAmount | Adjustment Tax Amount | double | TRUE | precision 18 scale 2 | |
| AdjustmentAmountWithTax | Adjustment Amount with Tax | double | TRUE | precision 18 scale 2 | |
| TaxTreatmentId | Tax Treatment ID | varchar | TRUE | 18 | |
| UnitOfMeasureId | Unit of Measure ID | varchar | TRUE | 18 | |
| ShippingAddressId | Invoice Address Group ID | varchar | TRUE | 18 | |
| BillingAddressId | Invoice Address Group ID | varchar | TRUE | 18 | |
| TaxProcessingStatus | Tax Processing Status | varchar | TRUE | 255 | |
| ConvertedNegAmount | Converted Negative Amount | double | TRUE | precision 18 scale 2 | |
| LegalEntityId | Legal Entity ID | varchar | TRUE | 18 | |
| LegalEntityAccountingPeriodId | Legal Entity Accounting Period ID | varchar | TRUE | 18 | |
| ChargeConvertedNegAmount | Charge Converted Negative Amount | double | TRUE | precision 18 scale 2 | |
| UsageProductId | Product ID | varchar | TRUE | 18 | |
| UsageProductBillSchdGrpId | Billing Schedule Group ID | varchar | TRUE | 18 | |
| ShipFromAddressId | Invoice Address Group ID | varchar | TRUE | 18 | |
| Billing_Treatment_Type__c | Billing Treatment Type | varchar | TRUE | 255 | |
| **Business_Unit_BU__c** | Business Unit (BU) | varchar | TRUE | 255 | Current bu source |
| **Department_ID_DID__c** | Department ID (DID) | varchar | TRUE | 255 | Current did source |
| PO_Number__c | PO Number | varchar | TRUE | 255 | |
| Subscription_Number__c | Subscription Number | varchar | TRUE | 255 | |
| Zuora_Id__c | Zuora Id | varchar | TRUE | 255 | |

---

## InvoiceLineTax

| Column Name | Column Label | Datatype | Nullable | Data Length | Notes |
|---|---|---|---|---|---|
| Id | Invoice Line Tax ID | varchar | FALSE | 18 | |
| IsDeleted | Deleted | bool | FALSE | | |
| InvoiceLineTaxNumber | Name | varchar | FALSE | 255 | |
| CreatedDate | Created Date | timestamp_tz | FALSE | | |
| CreatedById | Created By ID | varchar | FALSE | 18 | |
| LastModifiedDate | Last Modified Date | timestamp_tz | FALSE | | |
| LastModifiedById | Last Modified By ID | varchar | FALSE | 18 | |
| SystemModstamp | System Modstamp | timestamp_tz | FALSE | | |
| InvoiceLineId | Invoice Line ID | varchar | FALSE | 18 | |
| TaxAmount | Tax Amount | double | TRUE | precision 18 scale 2 | |
| Description | Description | varchar | TRUE | 255 | |
| StartDate | Start Date | date | FALSE | | |
| EndDate | End Date | date | FALSE | | |
| ConvertedNegAmount | Converted Negative Amount | double | TRUE | precision 18 scale 2 | |
| TaxName | Tax Name | varchar | TRUE | 255 | |
| TaxCode | Tax Code | varchar | TRUE | 255 | |
| TaxRate | Tax Rate | double | TRUE | precision 5 scale 2 | |
| TaxTransactionNumber | Tax Transaction Number | varchar | TRUE | 255 | |
| TaxDocumentNumber | Tax Document Number | varchar | TRUE | 255 | |
| TaxEffectiveDate | Tax Effective Date | date | TRUE | | |
| TaxTreatmentId | Tax Treatment ID | varchar | TRUE | 18 | |
| ShippingAddressId | Invoice Address Group ID | varchar | TRUE | 18 | |
| BillingAddressId | Invoice Address Group ID | varchar | TRUE | 18 | |
| TaxProcessingStatus | Tax Processing Status | varchar | TRUE | 255 | |
| LegalEntityId | Legal Entity ID | varchar | TRUE | 18 | |
| TaxExemptAmount | Tax Exempt Amount | double | TRUE | precision 18 scale 2 | |
| LegalEntityAccountingPeriodId | Legal Entity Accounting Period ID | varchar | TRUE | 18 | |
| ShipFromAddressId | Invoice Address Group ID | varchar | TRUE | 18 | |

---

## Payment

| Column Name | Column Label | Datatype | Nullable | Data Length | Notes |
|---|---|---|---|---|---|
| Id | Payment ID | varchar | FALSE | 18 | |
| IsDeleted | Deleted | bool | FALSE | | |
| PaymentNumber | Payment Number | varchar | FALSE | 255 | |
| CreatedDate | Created Date | timestamp_tz | FALSE | | |
| CreatedById | Created By ID | varchar | FALSE | 18 | |
| LastModifiedDate | Last Modified Date | timestamp_tz | FALSE | | |
| LastModifiedById | Last Modified By ID | varchar | FALSE | 18 | |
| SystemModstamp | System Modstamp | timestamp_tz | FALSE | | |
| LastViewedDate | Last Viewed Date | timestamp_tz | TRUE | | |
| LastReferencedDate | Last Referenced Date | timestamp_tz | TRUE | | |
| PaymentGroupId | Payment Group ID | varchar | TRUE | 18 | |
| AccountId | Account ID | varchar | TRUE | 18 | |
| PaymentAuthorizationId | Payment Authorization ID | varchar | TRUE | 18 | |
| Date | Date | timestamp_tz | TRUE | | |
| CancellationDate | Cancellation Date | timestamp_tz | TRUE | | |
| Amount | Amount | double | FALSE | precision 18 scale 2 | |
| Status | Status | varchar | FALSE | 255 | |
| Type | Type | varchar | FALSE | 255 | |
| ProcessingMode | Processing Mode | varchar | FALSE | 255 | |
| GatewayRefNumber | Gateway Reference Number | varchar | TRUE | 255 | |
| ClientContext | Client Context | varchar | TRUE | 2000 | |
| GatewayResultCode | Gateway Result Code | varchar | TRUE | 64 | |
| SfResultCode | Salesforce Result Code | varchar | TRUE | 255 | |
| GatewayDate | Gateway Date | timestamp_tz | TRUE | | |
| CancellationGatewayRefNumber | Cancellation Gateway Reference Number | varchar | TRUE | 255 | |
| CancellationGatewayResultCode | Cancellation Gateway Result Code | varchar | TRUE | 64 | |
| CancellationSfResultCode | Cancellation Salesforce Result Code | varchar | TRUE | 64 | |
| CancellationGatewayDate | Cancellation Gateway Date | timestamp_tz | TRUE | | |
| Comments | Comments | varchar | TRUE | 1000 | |
| ImpactAmount | Impact Amount | double | TRUE | precision 18 scale 2 | |
| EffectiveDate | Effective Date | timestamp_tz | TRUE | | |
| CancellationEffectiveDate | Cancellation Effective Date | timestamp_tz | TRUE | | |
| GatewayResultCodeDescription | Gateway Result Code Description | varchar | TRUE | 255 | |
| GatewayRefDetails | Gateway Reference Details | varchar | TRUE | 1000 | |
| IpAddress | IP Address | varchar | TRUE | 39 | |
| MacAddress | MAC Address | varchar | TRUE | 32 | |
| Phone | Phone | varchar | TRUE | 40 | |
| Email | Audit Email | varchar | TRUE | 80 | |
| PaymentGatewayId | Payment Gateway ID | varchar | TRUE | 18 | |
| PaymentMethodId | Payment Method ID | varchar | TRUE | 18 | |
| TotalApplied | Total Applied | double | TRUE | precision 18 scale 2 | |
| TotalUnapplied | Total Unapplied | double | TRUE | precision 18 scale 2 | |
| NetApplied | Net Applied | double | TRUE | precision 18 scale 2 | |
| Balance | Balance | double | TRUE | precision 18 scale 2 | |
| TotalRefundApplied | Total Refund Applied | double | TRUE | precision 18 scale 2 | |
| TotalRefundUnapplied | Total Refund Unapplied | double | TRUE | precision 18 scale 2 | |
| NetRefundApplied | Net Refund Applied | double | TRUE | precision 18 scale 2 | |
| PaymentIntentGuid | Payment Intent Guid | varchar | TRUE | 255 | |
| LegalEntityId | Legal Entity ID | varchar | TRUE | 18 | |
| LegalEntityAccountingPeriodId | Legal Entity Accounting Period ID | varchar | TRUE | 18 | |
| PaymentInitiationSourceId | Payment Initiation Source ID | varchar | TRUE | 18 | |
| CorporateCurrencyCvsnRate | Corporate Currency Conversion Rate | double | TRUE | precision 18 scale 10 | |
| CorporateCurrencyCvsnDate | Corporate Currency Conversion Date | date | TRUE | | |
| CorporateCurrencyCnvAmount | Corporate Currency Converted Amount | double | TRUE | precision 18 scale 6 | |
| TotalPaymentCreditApplied | Total Payment Credit Applied | double | TRUE | precision 18 scale 2 | |
| TotalPaymentCreditUnapplied | Total Payment Credit Unapplied | double | TRUE | precision 18 scale 2 | |
| NetPaymentCreditApplied | Net Payment Credit Applied | double | TRUE | precision 18 scale 2 | |
| Advanced_Payment__c | Advanced Payment | bool | FALSE | | |
| Payment_Type__c | Payment Type | varchar | TRUE | 255 | |
| Zuora_Id__c | Zuora Id | varchar | TRUE | 255 | |

*(Only `Id` is currently used by `gl_source_join.py` — the Refund/RefundLinePayment join hop.)*

---

## PaymentLineInvoice

| Column Name | Column Label | Datatype | Nullable | Data Length | Notes |
|---|---|---|---|---|---|
| Id | Payment Line Invoice ID | varchar | FALSE | 18 | |
| IsDeleted | Deleted | bool | FALSE | | |
| PaymentLineInvoiceNumber | Payment Line Invoice Number | varchar | FALSE | 255 | |
| CreatedDate | Created Date | timestamp_tz | FALSE | | |
| CreatedById | Created By ID | varchar | FALSE | 18 | |
| LastModifiedDate | Last Modified Date | timestamp_tz | FALSE | | |
| LastModifiedById | Last Modified By ID | varchar | FALSE | 18 | |
| SystemModstamp | System Modstamp | timestamp_tz | FALSE | | |
| LastViewedDate | Last Viewed Date | timestamp_tz | TRUE | | |
| LastReferencedDate | Last Referenced Date | timestamp_tz | TRUE | | |
| InvoiceId | Invoice ID | varchar | FALSE | 18 | |
| PaymentId | Payment ID | varchar | FALSE | 18 | |
| Amount | Amount | double | FALSE | precision 18 scale 2 | |
| Type | Type | varchar | FALSE | 255 | |
| HasBeenUnapplied | Has Been Unapplied | varchar | FALSE | 255 | |
| Comments | Comments | varchar | TRUE | 1000 | |
| Date | Date | timestamp_tz | TRUE | | |
| AppliedDate | Applied Date | timestamp_tz | TRUE | | |
| EffectiveDate | Effective Date | timestamp_tz | TRUE | | |
| UnappliedDate | Unapplied Date | timestamp_tz | TRUE | | |
| AssociatedAccountId | Account ID | varchar | TRUE | 18 | |
| AssociatedPaymentLineId | Payment Line Invoice ID | varchar | TRUE | 18 | |
| ImpactAmount | Impact Amount | double | TRUE | precision 18 scale 2 | |
| EffectiveImpactAmount | Effective Impact Amount | double | TRUE | precision 18 scale 2 | |
| PaymentBalance | Payment Balance | double | TRUE | precision 18 scale 2 | |
| LegalEntityId | Legal Entity ID | varchar | TRUE | 18 | |
| LegalEntityAccountingPeriodId | Legal Entity Accounting Period ID | varchar | TRUE | 18 | |

**CONFIRMED 864 rows** — this is the working header-level path for Payment resolution today.

---

## PaymentLineInvoiceLine

| Column Name | Column Label | Datatype | Nullable | Data Length | Notes |
|---|---|---|---|---|---|
| Id | Payment Line Invoice Line ID | varchar | FALSE | 18 | |
| IsDeleted | Deleted | bool | FALSE | | |
| PaymentLineInvoiceLineNumber | Name | varchar | FALSE | 255 | |
| CreatedDate | Created Date | timestamp | FALSE | | |
| CreatedById | Created By ID | varchar | FALSE | 18 | |
| LastModifiedDate | Last Modified Date | timestamp | FALSE | | |
| LastModifiedById | Last Modified By ID | varchar | FALSE | 18 | |
| SystemModstamp | System Modstamp | timestamp | FALSE | | |
| LastViewedDate | Last Viewed Date | timestamp | TRUE | | |
| LastReferencedDate | Last Referenced Date | timestamp | TRUE | | |
| InvoiceLineId | Invoice Line ID | varchar | FALSE | 18 | |
| PaymentId | Payment ID | varchar | FALSE | 18 | |
| Amount | Amount | double | FALSE | precision 18 scale 2 | |
| Type | Type | varchar | FALSE | 255 | |
| UnappliedStatus | Unapplied Status | varchar | FALSE | 255 | |
| Description | Description | varchar | TRUE | 1000 | |
| AppliedDateTime | Applied Date Time | timestamp | TRUE | | |
| EffectiveDateTime | Effective Date Time | timestamp | TRUE | | |
| UnappliedDateTime | Unapplied Date Time | timestamp | TRUE | | |
| AccountId | Account ID | varchar | TRUE | 18 | |
| RelatedPaymentLineInvcLineId | Payment Line Invoice Line ID | varchar | TRUE | 18 | |
| ImpactAmount | Impact Amount | double | TRUE | precision 18 scale 2 | |
| AppliedImpactAmount | Applied Impact Amount | double | TRUE | precision 18 scale 2 | |
| PaymentBalance | Payment Balance | double | TRUE | precision 18 scale 2 | |
| InvoiceLineBalance | Invoice Line Balance | double | TRUE | precision 18 scale 2 | |
| LegalEntityId | Legal Entity ID | varchar | TRUE | 18 | |
| LegalEntityAccountingPeriodId | Legal Entity Accounting Period ID | varchar | TRUE | 18 | |

**CONFIRMED 0 rows** — the line-level fallback that never actually fires today.

---

## Product2

| Column Name | Column Label | Datatype | Nullable | Data Length | Notes |
|---|---|---|---|---|---|
| Id | Product ID | varchar | FALSE | 18 | |
| Name | Product Name | varchar | FALSE | 255 | |
| ProductCode | Product Code | varchar | TRUE | 255 | |
| Description | Product Description | varchar | TRUE | 4000 | |
| IsActive | Active | bool | FALSE | | |
| CreatedDate | Created Date | timestamp_tz | FALSE | | |
| CreatedById | Created By ID | varchar | FALSE | 18 | |
| LastModifiedDate | Last Modified Date | timestamp_tz | FALSE | | |
| LastModifiedById | Last Modified By ID | varchar | FALSE | 18 | |
| SystemModstamp | System Modstamp | timestamp_tz | FALSE | | |
| Family | Product Family | varchar | TRUE | 255 | |
| TaxPolicyId | Tax Policy ID | varchar | TRUE | 18 | |
| BillingPolicyId | Billing Policy ID | varchar | TRUE | 18 | |
| ExternalDataSourceId | External Data Source ID | varchar | TRUE | 18 | |
| ExternalId | External ID | varchar | TRUE | 255 | |
| DisplayUrl | Display URL | varchar | TRUE | 1000 | |
| QuantityUnitOfMeasure | Quantity Unit Of Measure | varchar | TRUE | 255 | |
| IsDeleted | Deleted | bool | FALSE | | |
| IsArchived | Archived | bool | FALSE | | |
| LastViewedDate | Last Viewed Date | timestamp_tz | TRUE | | |
| LastReferencedDate | Last Referenced Date | timestamp_tz | TRUE | | |
| StockKeepingUnit | Product SKU | varchar | TRUE | 180 | |
| Type | Product Type | varchar | TRUE | 40 | |
| AvailabilityDate | Availability Date | timestamp_tz | TRUE | | |
| DiscontinuedDate | Discontinued Date | timestamp_tz | TRUE | | |
| BasedOnId | Product Classification ID | varchar | TRUE | 18 | |
| EndOfLifeDate | End Of Life Date | timestamp_tz | TRUE | | |
| HelpText | Help Text | varchar | TRUE | 32768 | |
| IsAssetizable | Is Assetizable | bool | FALSE | | |
| ConfigureDuringSale | Configure During Sale | varchar | TRUE | 40 | |
| IsSoldOnlyWithOtherProds | Sell only with other products | bool | FALSE | | |
| SpecificationType | Specification Type | varchar | TRUE | 255 | |
| CanRamp | Ramp | bool | FALSE | | |
| UsageModelType | Usage Model Type | varchar | TRUE | 40 | |
| UnitOfMeasureId | Unit of Measure ID | varchar | TRUE | 18 | |
| Price_Type__c | Price Type | varchar | TRUE | 255 | |
| **Business_Unit_BU__c** | Business Unit (BU) | varchar | TRUE | 255 | **CONFIRMED to exist** — same field name as InvoiceLine's |
| **Department_ID_DID__c** | Department ID (DID) | varchar | TRUE | 255 | **CONFIRMED to exist** — same field name as InvoiceLine's |

*(**Now loaded by `build_source_dataframe()`** — Product2 is the
PREFERRED bu/did source, universally, per Dakota: "it's better to go
through product2 than invoice line for the bu and did fields." Real
dataset_id confirmed: `29351664-7f3c-4266-8937-018cc5a7dd44`. InvoiceLine's
own bu/did is now the fallback, used only where Product2Id doesn't
resolve or these fields come back null.)*

---

## Refund

| Column Name | Column Label | Datatype | Nullable | Data Length | Notes |
|---|---|---|---|---|---|
| Id | Refund ID | varchar | FALSE | 18 | |
| IsDeleted | Deleted | bool | FALSE | | |
| RefundNumber | Refund Number | varchar | FALSE | 255 | |
| CreatedDate | Created Date | timestamp | FALSE | | |
| CreatedById | Created By ID | varchar | FALSE | 18 | |
| LastModifiedDate | Last Modified Date | timestamp | FALSE | | |
| LastModifiedById | Last Modified By ID | varchar | FALSE | 18 | |
| SystemModstamp | System Modstamp | timestamp | FALSE | | |
| LastViewedDate | Last Viewed Date | timestamp | TRUE | | |
| LastReferencedDate | Last Referenced Date | timestamp | TRUE | | |
| Type | Type | varchar | FALSE | 255 | |
| PaymentGroupId | Payment Group ID | varchar | TRUE | 18 | |
| ImpactAmount | Impact Amount | double | TRUE | precision 18 scale 2 | |
| ProcessingMode | Processing Mode | varchar | FALSE | 255 | |
| Amount | Amount | double | FALSE | precision 18 scale 2 | |
| AccountId | Account ID | varchar | TRUE | 18 | |
| PaymentMethodId | Payment Method ID | varchar | TRUE | 18 | |
| Comments | Comments | varchar | TRUE | 1000 | |
| Status | Status | varchar | FALSE | 255 | |
| GatewayRefNumber | Gateway Reference Number | varchar | TRUE | 255 | |
| ClientContext | Client Context | varchar | TRUE | 2000 | |
| GatewayResultCode | Gateway Result Code | varchar | TRUE | 64 | |
| GatewayResultCodeDescription | Gateway Result Code Description | varchar | TRUE | 255 | |
| SfResultCode | Salesforce Result Code | varchar | TRUE | 255 | |
| GatewayDate | Gateway Date | timestamp | TRUE | | |
| IpAddress | IP Address | varchar | TRUE | 39 | |
| MacAddress | MAC Address | varchar | TRUE | 32 | |
| Phone | Phone | varchar | TRUE | 40 | |
| Email | Audit Email | varchar | TRUE | 80 | |
| EffectiveDate | Effective Date | timestamp | TRUE | | |
| Date | Date | timestamp | TRUE | | |
| CancellationEffectiveDate | Cancellation Effective Date | timestamp | TRUE | | |
| CancellationDate | Cancellation Date | timestamp | TRUE | | |
| CancellationGatewayRefNumber | Cancellation Gateway Reference Number | varchar | TRUE | 255 | |
| CancellationGatewayResultCode | Cancellation Gateway Result Code | varchar | TRUE | 64 | |
| CancellationSfResultCode | Cancellation Salesforce Result Code | varchar | TRUE | 64 | |
| CancellationGatewayDate | Cancellation Gateway Date | timestamp | TRUE | | |
| PaymentGatewayId | Payment Gateway ID | varchar | TRUE | 18 | |
| TotalApplied | Total Applied | double | TRUE | precision 18 scale 2 | |
| TotalUnapplied | Total Unapplied | double | TRUE | precision 18 scale 2 | |
| NetApplied | Net Applied | double | TRUE | precision 18 scale 2 | |
| Balance | Balance | double | TRUE | precision 18 scale 2 | |
| LegalEntityId | Legal Entity ID | varchar | TRUE | 18 | |
| LegalEntityAccountingPeriodId | Legal Entity Accounting Period ID | varchar | TRUE | 18 | |

*(Only `Id` is currently used by `gl_source_join.py` — the "Refund" TransactionType entry point.)*

---

## RefundLinePayment

| Column Name | Column Label | Datatype | Nullable | Data Length | Notes |
|---|---|---|---|---|---|
| Id | Refund Line Payment ID | varchar | FALSE | 18 | |
| IsDeleted | Deleted | bool | FALSE | | |
| RefundLinePaymentNumber | Refund Line Payment Number | varchar | FALSE | 255 | |
| CreatedDate | Created Date | timestamp | FALSE | | |
| CreatedById | Created By ID | varchar | FALSE | 18 | |
| LastModifiedDate | Last Modified Date | timestamp | FALSE | | |
| LastModifiedById | Last Modified By ID | varchar | FALSE | 18 | |
| SystemModstamp | System Modstamp | timestamp | FALSE | | |
| **PaymentId** | Payment ID | varchar | FALSE | 18 | |
| **RefundId** | Refund ID | varchar | FALSE | 18 | |
| Amount | Amount | double | FALSE | precision 18 scale 2 | |
| Type | Type | varchar | FALSE | 255 | |
| HasBeenUnapplied | Has Been Unapplied | varchar | FALSE | 255 | |
| Comments | Comments | varchar | TRUE | 1000 | |
| Date | Date | timestamp | TRUE | | |
| AppliedDate | Applied Date | timestamp | TRUE | | |
| EffectiveDate | Effective Date | timestamp | TRUE | | |
| UnappliedDate | Unapplied Date | timestamp | TRUE | | |
| AssociatedAccountId | Account ID | varchar | TRUE | 18 | |
| AssociatedRefundLinePaymentId | Refund Line Payment ID | varchar | TRUE | 18 | |
| ImpactAmount | Impact Amount | double | TRUE | precision 18 scale 2 | |
| EffectiveImpactAmount | Effective Impact Amount | double | TRUE | precision 18 scale 2 | |
| RefundBalance | Refund Balance | double | TRUE | precision 18 scale 2 | |
| PaymentBalance | Payment Balance | double | TRUE | precision 18 scale 2 | |
| LegalEntityId | Legal Entity ID | varchar | TRUE | 18 | |
| LegalEntityAccountingPeriodId | Legal Entity Accounting Period ID | varchar | TRUE | 18 | |

`PaymentId`/`RefundId` both `nullable: FALSE` — matches
`EXPECTED_COLUMNS["refund_line_payment"]` exactly. Real dataset_id
registration still pending, per Dakota (see `DATASET_IDS` PLACEHOLDER note).

---

## TransactionJournal

| Column Name | Column Label | Datatype | Nullable | Data Length | Notes |
|---|---|---|---|---|---|
| Id | Transaction Journal ID | varchar | FALSE | 18 | |
| Name | Name | varchar | FALSE | 30 | |
| IsDeleted | Deleted | bool | FALSE | | |
| CreatedById | Created By ID | varchar | FALSE | 18 | |
| CreatedDate | Created Date | timestamp_tz | FALSE | | |
| LastModifiedDate | Last Modified Date | timestamp_tz | FALSE | | |
| LastModifiedById | Last Modified By ID | varchar | FALSE | 18 | |
| SystemModstamp | System Modstamp | timestamp_tz | FALSE | | |
| AccountId | Account ID | varchar | TRUE | 18 | confirmed NOT populated in practice |
| ActivityDate | Activity Date | timestamp_tz | FALSE | | |
| Status | Status | varchar | FALSE | 40 | |
| ExternalTransactionNumber | External Transaction ID | varchar | TRUE | 40 | |
| UsageType | Usage Type | varchar | TRUE | 40 | |
| StartDate | Start Date | timestamp_tz | TRUE | | |
| EndDate | End Date | timestamp_tz | TRUE | | |
| TransactionAmount | Transaction Amount | double | TRUE | precision 18 scale 2 | |
| Quantity | Quantity | double | TRUE | precision 18 scale 2 | |
| QuantityUnitOfMeasureId | Quantity Unit ID | varchar | TRUE | 18 | |
| ReferenceRecordId | Reference Record ID | varchar | TRUE | 18 | |
| UsageResourceId | Usage Resource ID | varchar | TRUE | 18 | dead end for bu/did — see module docstring |
| **TransactionType** | Transaction Type | varchar | TRUE | 40 | Names the table ReferenceTransactionRecordId points to |
| LegalEntityId | Legal Entity ID | varchar | TRUE | 18 | |
| **DebitGeneralLedgerAccountId** | General Ledger Account ID | varchar | TRUE | 18 | |
| **CreditGeneralLedgerAccountId** | General Ledger Account ID | varchar | TRUE | 18 | |
| **ReferenceTransactionRecordId** | Reference Transaction Record ID | varchar | TRUE | 18 | The polymorphic FK resolve_product2_id() walks |
| GeneralLedgerAcctAsgntRuleId | General Ledger Account Assignment Rule ID | varchar | TRUE | 18 | |
| UniqueIdentifier | Unique Identifier | varchar | TRUE | 80 | |
| LegalEntyAccountingPeriodId | Legal Entity Accounting Period ID | varchar | TRUE | 18 | |
| Credit | Credit | double | TRUE | precision 18 scale 2 | |
| Debit | Debit | double | TRUE | precision 18 scale 2 | |
| UsageSummaryId | Usage Summary ID | varchar | TRUE | 18 | |
| ForeignExchangeGainOrLossType | Foreign Exchange Gain Or Loss Type | varchar | TRUE | 40 | |
| GenlLdgrJournalEntryRuleId | General Ledger Account Assignment Rule ID | varchar | TRUE | 18 | |
| LastViewedDate | Last Viewed Date | timestamp_tz | TRUE | | |
| LastReferencedDate | Last Referenced Date | timestamp_tz | TRUE | | |
| Zuora_Id__c | Zuora Id | varchar | TRUE | 255 | |
