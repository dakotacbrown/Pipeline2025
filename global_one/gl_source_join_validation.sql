-- =============================================================================
-- GL Journal Source Join — SQL reference for validation
-- Regenerated to match helpers/gl_source_join.py as of this date. Mirrors
-- build_source_dataframe(), resolve_bu_did(), and
-- build_reference_to_account_lookup() exactly — each CTE below corresponds
-- to one Python function, in the same order, so this can be checked
-- line-by-line against the pipeline code.
--
-- Confirmed TransactionType values: InvoiceLineTax, Payment, CreditMemo, InvoiceLine
--
-- Output column names match build_source_dataframe()'s current Table.Column
-- naming exactly:
--   InvoiceLine.Business_Unit, InvoiceLine.Department_Id,
--   TransactionJournal.ActivityDate, TransactionJournal.TransactionType,
--   Account.AccountNumber, TransactionJournal.UsageType,
--   TransactionJournal.CreditDebit, TransactionJournal.Name
--
-- Table names below assume cadet.bronze.revcloud_* naming, matching the
-- validation queries already run against this environment. Adjust schema
-- prefix if the validator is checking against a different layer (silver/gold).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- CTE 1: resolve_bu_did()
--   Resolves InvoiceLine.Business_Unit / .Department_Id / Product2Id per
--   TransactionType.
--
--   InvoiceLine     -> ReferenceTransactionRecordId = InvoiceLine.Id directly
--   InvoiceLineTax  -> ReferenceTransactionRecordId = InvoiceLineTax.Id,
--                       then InvoiceLineTax.InvoiceLineId -> InvoiceLine
--   Payment         -> ReferenceTransactionRecordId = Payment.Id (header).
--                       (a) line-level: PaymentLineInvoiceLine -> InvoiceLine
--                           CONFIRMED 0 ROWS in current data
--                       (b) header-level: PaymentLineInvoice -> Invoice -> InvoiceLine
--                           CONFIRMED 837 rows — this is what resolves Payment rows today
--   CreditMemo      -> ReferenceTransactionRecordId = CreditMemo.Id (header).
--                       (a) line-level: CreditMemoLine -> CreditMemoLineInvoiceLine -> InvoiceLine
--                           CONFIRMED 0 ROWS in current data
--                       (b) header-level: CreditMemoInvApplication -> Invoice -> InvoiceLine
--                           CONFIRMED 42 rows (matches CreditMemo row count) —
--                           this is what resolves CreditMemo rows today
--
--   NOTE: an earlier design routed bu/did through
--   TransactionJournal -> UsageResource -> RateCardEntry -> Product2, but
--   that was abandoned — UsageResourceId/Product2Id were confirmed to
--   always point to the same product regardless of transaction, a dead end
--   for differentiating bu/did. Not included below; not used by the
--   current pipeline at all.
--
--   PRIORITY: line-level paths win over header-level paths when both exist
--   for the same header id (kept in case line-level tables get populated
--   later).
--
--   CAVEAT: both header-level fallbacks join through Invoice -> InvoiceLine,
--   and one Invoice can have multiple InvoiceLines. Where those lines span
--   more than one bu/did, this takes the FIRST match (arbitrary tiebreak,
--   not a "correct" one — there isn't a single correct answer at header
--   level). See the two "-- FAN-OUT CHECK" queries at the bottom of this
--   file — these were run against real data and came back showing this
--   fan-out does NOT currently happen, but worth rerunning periodically as
--   data volume grows.
-- -----------------------------------------------------------------------------

with il_direct as (
    -- InvoiceLine direct
    select
        id as reference_transaction_record_id,
        product2id,
        business_unit_bu__c as business_unit,
        department_id_did__c as department_id,
        1 as source_priority  -- line-level
    from cadet.bronze.revcloud_invoice_line
),

il_via_tax as (
    -- InvoiceLineTax -> InvoiceLine
    select
        ilt.id as reference_transaction_record_id,
        il.product2id,
        il.business_unit_bu__c as business_unit,
        il.department_id_did__c as department_id,
        1 as source_priority  -- line-level
    from cadet.bronze.revcloud_invoice_line_tax ilt
    left join cadet.bronze.revcloud_invoice_line il
        on ilt.invoicelineid = il.id
),

payment_via_line as (
    -- Payment (a): line-level, CONFIRMED 0 rows currently
    select
        pli.paymentid as reference_transaction_record_id,
        il.product2id,
        il.business_unit_bu__c as business_unit,
        il.department_id_did__c as department_id,
        1 as source_priority  -- line-level
    from cadet.bronze.revcloud_payment_line_invoice_line pli
    left join cadet.bronze.revcloud_invoice_line il
        on pli.invoicelineid = il.id
),

payment_via_header as (
    -- Payment (b): header-level, this is what actually resolves Payment rows today
    select
        pli.paymentid as reference_transaction_record_id,
        il.product2id,
        il.business_unit_bu__c as business_unit,
        il.department_id_did__c as department_id,
        2 as source_priority  -- header-level fallback
    from cadet.bronze.revcloud_payment_line_invoice pli
    left join cadet.bronze.revcloud_invoice_line il
        on pli.invoiceid = il.invoiceid
),

credit_memo_via_line as (
    -- CreditMemo (a): line-level, CONFIRMED 0 rows currently
    select
        cml.creditmemoid as reference_transaction_record_id,
        il.product2id,
        il.business_unit_bu__c as business_unit,
        il.department_id_did__c as department_id,
        1 as source_priority  -- line-level
    from cadet.bronze.revcloud_credit_memo_line cml
    left join cadet.bronze.revcloud_credit_memo_line_invoice_line cmli
        on cmli.creditmemolineid = cml.id
    left join cadet.bronze.revcloud_invoice_line il
        on cmli.invoicelineid = il.id
),

credit_memo_via_header as (
    -- CreditMemo (b): header-level, this is what actually resolves CreditMemo rows today
    select
        cmia.creditmemoid as reference_transaction_record_id,
        il.product2id,
        il.business_unit_bu__c as business_unit,
        il.department_id_did__c as department_id,
        2 as source_priority  -- header-level fallback
    from cadet.bronze.revcloud_credit_memo_inv_application cmia
    left join cadet.bronze.revcloud_invoice_line il
        on cmia.invoiceid = il.invoiceid
),

bu_did_unioned as (
    select * from il_direct
    union all
    select * from il_via_tax
    union all
    select * from payment_via_line
    union all
    select * from payment_via_header
    union all
    select * from credit_memo_via_line
    union all
    select * from credit_memo_via_header
),

bu_did_ranked as (
    -- Line-level (priority 1) wins over header-level (priority 2) for the
    -- same reference_transaction_record_id. Where multiple rows tie at the
    -- same priority (fan-out through Invoice -> InvoiceLine), this takes an
    -- arbitrary first row — matches pandas drop_duplicates(keep='first').
    select
        *,
        row_number() over (
            partition by reference_transaction_record_id
            order by source_priority asc
        ) as rn
    from bu_did_unioned
),

bu_did_lookup as (
    select
        reference_transaction_record_id,
        product2id,
        business_unit,
        department_id
    from bu_did_ranked
    where rn = 1
),


-- -----------------------------------------------------------------------------
-- CTE 2: build_reference_to_account_lookup()
--   Resolves Account.AccountNumber per TransactionType, via the same
--   polymorphic ReferenceTransactionRecordId. Header-level types (Payment,
--   CreditMemo, Refund, Invoice) resolve directly. Line-level types
--   (InvoiceLine, InvoiceLineTax) need to walk up to Invoice first — this
--   was a BUG in the original implementation (fixed): InvoiceLine.Id/
--   InvoiceLineTax.Id were being matched directly against Invoice.Id,
--   which never matches since Salesforce record IDs are unique per object.
--   Every InvoiceLine/InvoiceLineTax row was getting a null account until
--   this was fixed.
--
--   CONFIRMED FIELD: Account.AccountNumber, NOT Account.Name. Real values
--   look like "A00000213" (confirmed via Salesforce API response) —
--   stripping the leading "A" gives an 8-digit code that fits the Journal
--   Line Account field's 10-character width. Account.Name was originally
--   used here but routinely exceeds 10 characters and was silently
--   truncating in the actual output file.
--
--   Salesforce IDs are globally unique across objects, so unioning all
--   (id -> account_id) pairs and matching once is safe — no risk of an
--   InvoiceLine.Id colliding with a Payment.Id, etc.
-- -----------------------------------------------------------------------------

ref_to_account_direct as (
    select id as reference_transaction_record_id, billingaccountid as account_id
    from cadet.bronze.revcloud_invoice
    union all
    select id as reference_transaction_record_id, billingaccountid as account_id
    from cadet.bronze.revcloud_credit_memo
    union all
    select id as reference_transaction_record_id, accountid as account_id
    from cadet.bronze.revcloud_payment
    union all
    select id as reference_transaction_record_id, accountid as account_id
    from cadet.bronze.revcloud_refund
),

ref_to_account_via_invoice_line as (
    -- BUG FIX: InvoiceLine.Id -> Invoice.Id -> BillingAccountId
    select
        il.id as reference_transaction_record_id,
        inv.billingaccountid as account_id
    from cadet.bronze.revcloud_invoice_line il
    left join cadet.bronze.revcloud_invoice inv
        on il.invoiceid = inv.id
),

ref_to_account_via_invoice_line_tax as (
    -- BUG FIX: InvoiceLineTax.Id -> InvoiceLine.Id -> Invoice.Id -> BillingAccountId
    select
        ilt.id as reference_transaction_record_id,
        inv.billingaccountid as account_id
    from cadet.bronze.revcloud_invoice_line_tax ilt
    left join cadet.bronze.revcloud_invoice_line il
        on ilt.invoicelineid = il.id
    left join cadet.bronze.revcloud_invoice inv
        on il.invoiceid = inv.id
),

ref_to_account_unioned as (
    select * from ref_to_account_direct
    union all
    select * from ref_to_account_via_invoice_line
    union all
    select * from ref_to_account_via_invoice_line_tax
),

ref_to_account_deduped as (
    -- Salesforce IDs are globally unique, so there should never be a real
    -- collision here — dedup is defensive, matching the pandas
    -- drop_duplicates(subset="Id") behavior.
    select distinct reference_transaction_record_id, account_id
    from ref_to_account_unioned
),

account_lookup as (
    select
        r.reference_transaction_record_id,
        -- clean_account_number(): strip a single leading "A" character only
        -- (confirmed via real AccountNumber values like "A00000213")
        case
            when a.accountnumber like 'A%' then substring(a.accountnumber, 2)
            else a.accountnumber
        end as account_number
    from ref_to_account_deduped r
    left join cadet.bronze.revcloud_account a
        on r.account_id = a.id
),


-- -----------------------------------------------------------------------------
-- CTE 3: resolve_amount()
--   TransactionJournal.CreditDebit: Credit (negative) or Debit (positive),
--   whichever is populated. If both are populated, Credit takes priority
--   (matches pandas combine_first behavior in resolve_amount()). This is a
--   DERIVED value, not a literal field on TransactionJournal.
-- -----------------------------------------------------------------------------

tj_with_amount as (
    select
        *,
        case
            when credit is not null then -credit
            else debit
        end as creditdebit
    from cadet.bronze.revcloud_transaction_journal
)


-- -----------------------------------------------------------------------------
-- FINAL: build_source_dataframe()
--   Joins TransactionJournal to both lookups above. Output column names
--   match the Python's current Table.Column naming exactly.
-- -----------------------------------------------------------------------------

select
    bd.business_unit as "InvoiceLine.Business_Unit",
    bd.department_id as "InvoiceLine.Department_Id",
    tj.activitydate as "TransactionJournal.ActivityDate",
    tj.transactiontype as "TransactionJournal.TransactionType",
    al.account_number as "Account.AccountNumber",
    tj.usagetype as "TransactionJournal.UsageType",
    tj.creditdebit as "TransactionJournal.CreditDebit",
    tj.name as "TransactionJournal.Name"
from tj_with_amount tj
left join bu_did_lookup bd
    on tj.referencetransactionrecordid = bd.reference_transaction_record_id
left join account_lookup al
    on tj.referencetransactionrecordid = al.reference_transaction_record_id;


-- =============================================================================
-- VALIDATION QUERIES — run these separately to sanity-check the join above
-- =============================================================================

-- 1. How many TransactionJournal rows end up with a NULL business_unit
--    after the join? Should be low/zero given TransactionType is fully
--    covered by the CTEs above. Any nulls here mean either a data gap
--    (e.g. an Invoice/InvoiceLine with no matching record) or a fifth
--    TransactionType value that isn't handled yet — check transactiontype
--    on any null rows.
-- select transactiontype, count(*) as null_bu_count
-- from tj_with_amount tj
-- left join bu_did_lookup bd on tj.referencetransactionrecordid = bd.reference_transaction_record_id
-- where bd.business_unit is null
-- group by transactiontype;

-- 2. Same check for account_number nulls.
-- select tj.transactiontype, count(*) as null_account_count
-- from tj_with_amount tj
-- left join account_lookup al on tj.referencetransactionrecordid = al.reference_transaction_record_id
-- where al.account_number is null
-- group by tj.transactiontype;

-- 3. FAN-OUT CHECK — Payment: does a single Payment's applied invoice ever
--    span multiple department_ids? (Only department_id varies meaningfully
--    in this data; business_unit is currently constant across the whole
--    org.) CONFIRMED as of the last check: this does NOT currently happen —
--    rerun periodically as data volume grows.
-- select pli.paymentid, count(distinct il.department_id_did__c) as distinct_depts_touched
-- from cadet.bronze.revcloud_payment_line_invoice pli
-- left join cadet.bronze.revcloud_invoice_line il on pli.invoiceid = il.invoiceid
-- group by pli.paymentid
-- having count(distinct il.department_id_did__c) > 1;

-- 4. FAN-OUT CHECK — CreditMemo: same check via CreditMemoInvApplication.
--    CONFIRMED as of the last check: this does NOT currently happen —
--    rerun periodically as data volume grows.
-- select cmia.creditmemoid, count(distinct il.department_id_did__c) as distinct_depts_touched
-- from cadet.bronze.revcloud_credit_memo_inv_application cmia
-- left join cadet.bronze.revcloud_invoice_line il on cmia.invoiceid = il.invoiceid
-- group by cmia.creditmemoid
-- having count(distinct il.department_id_did__c) > 1;

-- 5. Row count sanity check — total output rows should equal total
--    TransactionJournal rows (this is a left join chain, so no row should
--    be dropped or duplicated by the joins themselves; duplication would
--    indicate a many-to-one join went wrong somewhere above).
-- select
--   (select count(*) from cadet.bronze.revcloud_transaction_journal) as tj_row_count,
--   (select count(*) from ( <paste the FINAL select above> ) x) as output_row_count;

-- 6. Sanity check on Account.AccountNumber format — confirms the "strip
--    leading A" logic is actually matching real data shape.
-- select accountnumber, substring(accountnumber, 2) as stripped
-- from cadet.bronze.revcloud_account
-- limit 20;
