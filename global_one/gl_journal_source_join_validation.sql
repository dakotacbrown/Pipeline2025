-- =============================================================================
-- GL Journal Source Join — SQL reference for validation
-- Regenerated to match helpers/gl_source_join.py as of this date. Mirrors
-- resolve_product2_id(), resolve_product2_fields(), apply_product2_bu_did(),
-- resolve_gl_accounting_number(), apply_did_overrides(), resolve_amount(),
-- and validate_required_tables_present() — each CTE below corresponds to
-- one Python function, in the same order, so this can be checked
-- line-by-line against the pipeline code.
--
-- MAJOR REWRITE FROM THE PRIOR VERSION OF THIS FILE:
--   - Product2 is now the SOLE bu/did source, universally, per Dakota:
--     "Product2 should not have a null or blank did/bu... it's okay to
--     remove the invoice line resolution." InvoiceLine's own
--     Business_Unit_BU__c/Department_ID_DID__c fields are NOT read for
--     bu/did purposes at all anymore — an earlier version of this file
--     used InvoiceLine as a two-tier fallback; that layer is gone.
--   - All 15 confirmed TransactionType picklist values now have a
--     resolution path (CTE 1) — the prior version only covered 7.
--   - The slingshot/databolt did override now checks Product2.Name, not
--     InvoiceLine.Name — per Dakota, "we shouldn't need invoice line name
--     anymore for the name check."
--   - New CTE 7: mirrors validate_required_tables_present() — per Dakota:
--     "If the transaction [journal], product2, general ledger account
--     tables, or any table under a distinct list from transaction type
--     are empty it should fail. That would mean a data issue is
--     present." See that CTE's own comment for the exact OR-path logic
--     (a type only fails if EVERY one of its resolution paths is dead,
--     not just one of several fallbacks).
--   - REMOVED: the CreditMemo "line-level via CreditMemoLineInvoiceLine"
--     candidate — confirmed structurally DEAD, not just untested. It
--     derived from the same CreditMemoLine table, keyed by the same
--     CreditMemoId, as the DIRECT candidate, which is always tried first
--     — so the junction-table path could never actually change the
--     result, even in the one case (a null CreditMemoLine.Product2Id
--     with a real answer reachable via the junction) where it would have
--     mattered. CreditMemo now has 2 paths (direct, header), not 3.
--
-- Confirmed TransactionType values (15 total, per Dakota's screenshots of
-- the field's full picklistValues): InvoiceLine, InvoiceLineTax,
-- DebitMemoLine, Payment, CreditMemo, RefundLinePayment, Refund, Invoice,
-- CreditMemoLine, CreditMemoLineTax, PaymentLineInvoice,
-- PaymentLineInvoiceLine, CreditMemoInvApplication,
-- CreditMemoLineInvoiceLine, DebitMemoLineTax. Per Dakota, each
-- TransactionType value names the table ReferenceTransactionRecordId
-- points to directly — a quick way to identify the table without parsing
-- the Salesforce ID prefix. DebitMemoLineTax is the one exception worth
-- knowing: no corresponding table exists in Salesforce YET (per Dakota) —
-- the chain below is fully built anyway ("I'll still need to have the
-- mapping in case it goes live"), it just can't resolve anything today.
--
-- No schema/catalog prefix is assumed on any table reference below — add
-- whatever prefix your environment needs.
--
-- Output column names match build_source_dataframe()'s current Table.Column
-- naming exactly — NOTE: "InvoiceLine.Business_Unit"/"InvoiceLine.Department_Id"
-- are historical labels kept for the file spec's own field naming; despite
-- the name, both are sourced ENTIRELY from Product2 now, not InvoiceLine.
--   InvoiceLine.Business_Unit, InvoiceLine.Department_Id,
--   TransactionJournal.ActivityDate, TransactionJournal.TransactionType,
--   GeneralLedgerAccount.GL_Accounting_Number__c, TransactionJournal.UsageType,
--   TransactionJournal.CreditDebit, TransactionJournal.Name
-- =============================================================================


-- -----------------------------------------------------------------------------
-- CTE 0: ActivityDate window — resolve_date_window()
--   Month-to-date by default (midnight UTC on the 1st of the current month
--   through now), or substitute explicit literals for an ad-hoc range.
--   Applied to TransactionJournal BEFORE any of the joins below.
-- -----------------------------------------------------------------------------

with window_bounds as (
    select
        date_trunc('month', current_timestamp()) as window_start,
        current_timestamp() as window_end
    -- For an explicit range instead, replace the select above with:
    -- select
    --     timestamp('2026-08-01T00:00:00Z') as window_start,
    --     timestamp('2026-08-20T00:00:00Z') as window_end
),

tj_in_window as (
    select tj.*
    from TransactionJournal tj
    cross join window_bounds w
    where tj.ActivityDate >= w.window_start
      and tj.ActivityDate <= w.window_end
),


-- -----------------------------------------------------------------------------
-- CTE 1: resolve_product2_id()
--   Resolves Product2Id per TransactionType — the SOLE job of this stage.
--   InvoiceLine's own bu/did fields are never read here at all.
--
--   InvoiceLine       -> InvoiceLine.Product2Id directly
--   InvoiceLineTax    -> InvoiceLineTax.InvoiceLineId -> InvoiceLine.Product2Id
--   DebitMemoLine     -> DebitMemoLine.Product2Id directly
--   Payment           -> ReferenceTransactionRecordId = Payment.Id directly
--                        (does NOT require a matching row in the Payment
--                        table itself — confirmed via a real end-to-end
--                        run against fixture data that gating this on
--                        Payment.Id was a bug, not a feature — see the
--                        comment on the payment_via_line/header CTEs below)
--                        -> (a) PaymentLineInvoiceLine.PaymentId ->
--                        InvoiceLineId -> InvoiceLine.Product2Id
--                        (line-level, currently 0 rows) OR (b)
--                        PaymentLineInvoice.PaymentId -> InvoiceId ->
--                        InvoiceLine.InvoiceId -> InvoiceLine.Product2Id
--                        (header-level, what resolves data today)
--   CreditMemo        -> tried in this order: (a) DIRECT —
--                        CreditMemoLine.CreditMemoId =
--                        ReferenceTransactionRecordId ->
--                        CreditMemoLine.Product2Id (bypasses the junction
--                        table entirely); (b) header —
--                        CreditMemoInvApplication -> InvoiceLine.Product2Id.
--                        NOTE: there is deliberately NO "line-level via
--                        CreditMemoLineInvoiceLine" candidate — confirmed
--                        structurally DEAD, not just untested: it derived
--                        from the same CreditMemoLine table, keyed by the
--                        same CreditMemoId, as (a) above, which always
--                        wins the dedup (first candidate wins regardless
--                        of null value) — so that path could never
--                        actually change the result. Removed rather than
--                        kept as unreachable code.
--   RefundLinePayment -> direct entry (RefundLinePayment.Id) -> Payment
--                        (explicit join hop KEPT here, per Dakota: "keep
--                        payment just to make sure there's nothing lost
--                        in the joins" — this hop is specific to
--                        RefundLinePayment/Refund, NOT to Payment's own
--                        resolution above) -> same (a)/(b) split as
--                        Payment above
--   Refund            -> Refund.Id -> RefundLinePayment.RefundId (one hop
--                        earlier than RefundLinePayment above) -> Payment
--                        -> same (a)/(b) split
--   Invoice           -> Invoice.Id (= ReferenceTransactionRecordId
--                        directly, no separate Invoice table load needed)
--                        -> InvoiceLine.InvoiceId -> InvoiceLine.Product2Id
--   CreditMemoLine    -> direct entry (CreditMemoLine.Id) ->
--                        CreditMemoLine.Product2Id
--   CreditMemoLineTax -> CreditMemoLineTax.CreditMemoLineId ->
--                        CreditMemoLine.Id -> CreditMemoLine.Product2Id
--   PaymentLineInvoice -> direct entry (PaymentLineInvoice.Id) ->
--                        PaymentLineInvoice.InvoiceId ->
--                        InvoiceLine.InvoiceId -> InvoiceLine.Product2Id
--   PaymentLineInvoiceLine -> direct entry (PaymentLineInvoiceLine.Id) ->
--                        PaymentLineInvoiceLine.InvoiceLineId ->
--                        InvoiceLine.Id -> InvoiceLine.Product2Id
--   CreditMemoInvApplication -> direct entry (CreditMemoInvApplication.Id)
--                        -> CreditMemoInvApplication.CreditMemoId ->
--                        CreditMemoLine (via CreditMemoLine.CreditMemoId)
--                        -> CreditMemoLine.Product2Id
--   CreditMemoLineInvoiceLine -> direct entry
--                        (CreditMemoLineInvoiceLine.Id) ->
--                        CreditMemoLineInvoiceLine.CreditMemoLineId ->
--                        CreditMemoLine.Id -> CreditMemoLine.Product2Id
--   DebitMemoLineTax  -> DebitMemoLineTax.DebitMemoLineId ->
--                        DebitMemoLine.Id -> DebitMemoLine.Product2Id. No
--                        corresponding table exists in Salesforce YET (per
--                        Dakota) — built anyway ("I'll still need to have
--                        the mapping in case it goes live").
--
--   PRIORITY: within a TransactionType with multiple candidate paths
--   (e.g. Payment's line/header split), the
--   more direct/specific path is listed first, deduplicated keeping the
--   first match — matches this file's "line-level tried first"
--   convention. Fan-out (an Invoice/CreditMemo with multiple matching
--   lines) takes the first match — confirmed acceptable by Dakota ("just
--   pull everything in... it's better to have one row for everything").
-- -----------------------------------------------------------------------------

il_direct as (
    select
        Id as reference_transaction_record_id,
        Product2Id,
        1 as source_priority
    from InvoiceLine
),

il_via_tax as (
    select
        ilt.Id as reference_transaction_record_id,
        il.Product2Id,
        1 as source_priority
    from InvoiceLineTax ilt
    left join InvoiceLine il on ilt.InvoiceLineId = il.Id
),

dml_direct as (
    select
        Id as reference_transaction_record_id,
        Product2Id,
        1 as source_priority
    from DebitMemoLine
),

-- Payment (a)/(b): ReferenceTransactionRecordId = Payment.Id directly.
-- Deliberately does NOT join to the Payment table itself — see CTE 1's
-- header comment for why (a real bug caught by testing, not a design
-- choice this file is guessing at).
payment_via_line as (
    select
        pli.PaymentId as reference_transaction_record_id,
        il.Product2Id,
        1 as source_priority
    from PaymentLineInvoiceLine pli
    left join InvoiceLine il on pli.InvoiceLineId = il.Id
),

payment_via_header as (
    select
        pli.PaymentId as reference_transaction_record_id,
        il.Product2Id,
        2 as source_priority
    from PaymentLineInvoice pli
    left join InvoiceLine il on pli.InvoiceId = il.InvoiceId
),

-- CreditMemo (a): DIRECT — bypasses the junction table entirely.
credit_memo_direct as (
    select
        CreditMemoId as reference_transaction_record_id,
        Product2Id,
        1 as source_priority
    from CreditMemoLine
    where CreditMemoId is not null
),

-- CreditMemo (b): header-level fallback
credit_memo_via_header as (
    select
        cmia.CreditMemoId as reference_transaction_record_id,
        il.Product2Id,
        2 as source_priority
    from CreditMemoInvApplication cmia
    left join InvoiceLine il on cmia.InvoiceId = il.InvoiceId
),

-- RefundLinePayment (a)/(b): direct entry, THIS path (unlike Payment
-- above) does explicitly join through Payment — per Dakota: "keep
-- payment just to make sure there's nothing lost in the joins."
refund_line_payment_via_line as (
    select
        rlp.Id as reference_transaction_record_id,
        il.Product2Id,
        1 as source_priority
    from RefundLinePayment rlp
    left join Payment p on rlp.PaymentId = p.Id
    left join PaymentLineInvoiceLine pli on rlp.PaymentId = pli.PaymentId
    left join InvoiceLine il on pli.InvoiceLineId = il.Id
),

refund_line_payment_via_header as (
    select
        rlp.Id as reference_transaction_record_id,
        il.Product2Id,
        2 as source_priority
    from RefundLinePayment rlp
    left join Payment p on rlp.PaymentId = p.Id
    left join PaymentLineInvoice pli on rlp.PaymentId = pli.PaymentId
    left join InvoiceLine il on pli.InvoiceId = il.InvoiceId
),

-- Refund (a)/(b): one hop earlier than RefundLinePayment above
refund_via_line as (
    select
        r.Id as reference_transaction_record_id,
        il.Product2Id,
        1 as source_priority
    from Refund r
    left join RefundLinePayment rlp on r.Id = rlp.RefundId
    left join Payment p on rlp.PaymentId = p.Id
    left join PaymentLineInvoiceLine pli on rlp.PaymentId = pli.PaymentId
    left join InvoiceLine il on pli.InvoiceLineId = il.Id
),

refund_via_header as (
    select
        r.Id as reference_transaction_record_id,
        il.Product2Id,
        2 as source_priority
    from Refund r
    left join RefundLinePayment rlp on r.Id = rlp.RefundId
    left join Payment p on rlp.PaymentId = p.Id
    left join PaymentLineInvoice pli on rlp.PaymentId = pli.PaymentId
    left join InvoiceLine il on pli.InvoiceId = il.InvoiceId
),

-- Invoice: ReferenceTransactionRecordId = Invoice.Id directly, no
-- separate Invoice table load needed
invoice_direct as (
    select distinct
        InvoiceId as reference_transaction_record_id,
        Product2Id,
        1 as source_priority
    from InvoiceLine
    where InvoiceId is not null
),

-- CreditMemoLine: direct entry, its own standalone TransactionType
credit_memo_line_standalone as (
    select
        Id as reference_transaction_record_id,
        Product2Id,
        1 as source_priority
    from CreditMemoLine
),

-- CreditMemoLineTax -> CreditMemoLine
credit_memo_line_tax_standalone as (
    select
        cmlt.Id as reference_transaction_record_id,
        cml.Product2Id,
        1 as source_priority
    from CreditMemoLineTax cmlt
    left join CreditMemoLine cml on cmlt.CreditMemoLineId = cml.Id
),

-- PaymentLineInvoice: direct entry, its own standalone TransactionType
payment_line_invoice_standalone as (
    select
        pli.Id as reference_transaction_record_id,
        il.Product2Id,
        1 as source_priority
    from PaymentLineInvoice pli
    left join InvoiceLine il on pli.InvoiceId = il.InvoiceId
),

-- PaymentLineInvoiceLine: direct entry, its own standalone TransactionType
payment_line_invoice_line_standalone as (
    select
        pli.Id as reference_transaction_record_id,
        il.Product2Id,
        1 as source_priority
    from PaymentLineInvoiceLine pli
    left join InvoiceLine il on pli.InvoiceLineId = il.Id
),

-- CreditMemoInvApplication: direct entry, its own standalone TransactionType
credit_memo_inv_application_standalone as (
    select
        cmia.Id as reference_transaction_record_id,
        cml.Product2Id,
        1 as source_priority
    from CreditMemoInvApplication cmia
    left join CreditMemoLine cml on cmia.CreditMemoId = cml.CreditMemoId
),

-- CreditMemoLineInvoiceLine: direct entry, its own standalone TransactionType
credit_memo_line_invoice_line_standalone as (
    select
        cmli.Id as reference_transaction_record_id,
        cml.Product2Id,
        1 as source_priority
    from CreditMemoLineInvoiceLine cmli
    left join CreditMemoLine cml on cmli.CreditMemoLineId = cml.Id
),

-- DebitMemoLineTax -> DebitMemoLine. No corresponding table in Salesforce
-- yet, per Dakota — built anyway, always empty in practice today.
debit_memo_line_tax_standalone as (
    select
        dmlt.Id as reference_transaction_record_id,
        dml.Product2Id,
        1 as source_priority
    from DebitMemoLineTax dmlt
    left join DebitMemoLine dml on dmlt.DebitMemoLineId = dml.Id
),

product2_id_unioned as (
    select * from il_direct
    union all select * from il_via_tax
    union all select * from dml_direct
    union all select * from payment_via_line
    union all select * from payment_via_header
    union all select * from credit_memo_direct
    union all select * from credit_memo_via_header
    union all select * from refund_line_payment_via_line
    union all select * from refund_line_payment_via_header
    union all select * from refund_via_line
    union all select * from refund_via_header
    union all select * from invoice_direct
    union all select * from credit_memo_line_standalone
    union all select * from credit_memo_line_tax_standalone
    union all select * from payment_line_invoice_standalone
    union all select * from payment_line_invoice_line_standalone
    union all select * from credit_memo_inv_application_standalone
    union all select * from credit_memo_line_invoice_line_standalone
    union all select * from debit_memo_line_tax_standalone
),

product2_id_ranked as (
    select
        *,
        row_number() over (
            partition by reference_transaction_record_id
            order by source_priority asc
        ) as rn
    from product2_id_unioned
),

product2_id_lookup as (
    select reference_transaction_record_id, Product2Id
    from product2_id_ranked
    where rn = 1
),


-- -----------------------------------------------------------------------------
-- CTE 2: resolve_product2_fields()
--   Product2.Id -> .Business_Unit_BU__c / .Department_ID_DID__c / .Name.
--   .Name is used for the slingshot/databolt did override (CTE 6) —
--   replaces InvoiceLine.Name entirely, per Dakota.
-- -----------------------------------------------------------------------------

product2_fields as (
    select
        p2l.reference_transaction_record_id,
        p2.Business_Unit_BU__c as product2_bu,
        p2.Department_ID_DID__c as product2_did,
        p2.Name as product2_name
    from product2_id_lookup p2l
    left join Product2 p2 on p2l.Product2Id = p2.Id
),


-- -----------------------------------------------------------------------------
-- CTE 3: apply_product2_bu_did()
--   Direct assignment — Product2 is the SOLE bu/did source now, no
--   InvoiceLine fallback (an earlier version of this file combine_first'd
--   against one; that layer is gone, per Dakota: "it's okay to remove the
--   invoice line resolution").
--   (product2_fields above already IS the bu/did assignment — no separate
--   CTE needed here; apply_product2_bu_did() in the Python is a trivial
--   rename/copy of product2_bu/product2_did into bu/did, which this SQL
--   just reads directly from product2_fields in CTE 6 below.)
-- -----------------------------------------------------------------------------


-- -----------------------------------------------------------------------------
-- CTE 4: resolve_gl_accounting_number()
--   Resolves gl_accounting_number_c via TransactionJournal's OWN
--   DebitGeneralLedgerAccountId or CreditGeneralLedgerAccountId (whichever
--   is populated) -> GeneralLedgerAccount.Id ->
--   GeneralLedgerAccount.GL_Accounting_Number__c. Unrelated to CTE 1-3's
--   bu/did resolution entirely — a direct join off TransactionJournal.
-- -----------------------------------------------------------------------------

gl_account_lookup as (
    select
        tj.ReferenceTransactionRecordId as reference_transaction_record_id,
        gla.GL_Accounting_Number__c as gl_accounting_number_c
    from tj_in_window tj
    left join GeneralLedgerAccount gla
        on coalesce(tj.DebitGeneralLedgerAccountId, tj.CreditGeneralLedgerAccountId) = gla.Id
),


-- -----------------------------------------------------------------------------
-- CTE 5: resolve_amount()
--   TransactionJournal.CreditDebit: Credit (negated) or Debit, whichever
--   is populated. Credit takes priority if both are populated.
-- -----------------------------------------------------------------------------

tj_with_amount as (
    select
        *,
        case
            when Credit is not null then -Credit
            else Debit
        end as creditdebit
    from tj_in_window
),


-- -----------------------------------------------------------------------------
-- CTE 6: apply_did_overrides()
--   Applied AFTER CTE 1-3 (Product2's bu/did) are joined onto
--   TransactionJournal, in priority order (each step can overwrite the
--   one before it — listed low to high priority):
--     1. bu default: "10901" if still null. Expected to rarely fire now
--        (Product2 shouldn't have null/blank bu/did, per Dakota) but kept
--        as a defensive fallback.
--     2. did override by PRODUCT2 NAME (not InvoiceLine.Name, per Dakota
--        — "we shouldn't need invoice line name anymore for the name
--        check"): "16637" if Product2.Name contains "slingshot", or
--        "16635" if it contains "databolt" (databolt checked second, so
--        it wins if a name somehow matches both — confirmed directly
--        against the Python's sequential .loc overwrite order).
--     3. did override by GL account (HIGHEST priority): "16605" whenever
--        gl_accounting_number_c = "10040049".
-- -----------------------------------------------------------------------------

overrides_applied as (
    select
        tj.*,
        coalesce(nullif(trim(pf.product2_bu), ''), '10901') as resolved_business_unit,
        case
            when gl.gl_accounting_number_c = '10040049' then '16605'
            when lower(pf.product2_name) like '%databolt%' then '16635'
            when lower(pf.product2_name) like '%slingshot%' then '16637'
            else nullif(trim(pf.product2_did), '')
        end as resolved_department_id,
        gl.gl_accounting_number_c
    from tj_with_amount tj
    left join product2_fields pf
        on tj.ReferenceTransactionRecordId = pf.reference_transaction_record_id
    left join gl_account_lookup gl
        on tj.ReferenceTransactionRecordId = gl.reference_transaction_record_id
)


-- -----------------------------------------------------------------------------
-- FINAL: build_source_dataframe()
-- -----------------------------------------------------------------------------

select
    resolved_business_unit as "InvoiceLine.Business_Unit",
    resolved_department_id as "InvoiceLine.Department_Id",
    ActivityDate as "TransactionJournal.ActivityDate",
    TransactionType as "TransactionJournal.TransactionType",
    gl_accounting_number_c as "GeneralLedgerAccount.GL_Accounting_Number__c",
    UsageType as "TransactionJournal.UsageType",
    creditdebit as "TransactionJournal.CreditDebit",
    Name as "TransactionJournal.Name"
from overrides_applied;


-- =============================================================================
-- CTE 7 (separate query — run BEFORE the main query above):
-- validate_required_tables_present()
--   Per Dakota: "If the transaction [journal], product2, general ledger
--   account tables, or any table under a distinct list from transaction
--   type are empty it should fail. That would mean a data issue is
--   present." transaction_journal/product2/general_ledger_account are
--   unconditionally mandatory. For every other table, only the
--   TransactionType values ACTUALLY PRESENT in this run's
--   TransactionJournal data determine what's required — and a type only
--   fails if EVERY ONE of its possible resolution paths is dead, not if
--   just one of several fallbacks is empty (confirmed with Dakota
--   directly — e.g. PaymentLineInvoiceLine has been 0 rows this whole
--   project, known/expected, not a data issue, since Payment's
--   header-level path resolves fine on its own).
-- =============================================================================

-- 1. Mandatory tables — run this first. Any row here is a hard stop.
-- select 'transaction_journal' as table_name, count(*) as row_count from TransactionJournal
-- union all
-- select 'product2', count(*) from Product2
-- union all
-- select 'general_ledger_account', count(*) from GeneralLedgerAccount;

-- 2. Per-type path liveness — run this to check, for each TransactionType
--    ACTUALLY PRESENT in your data, whether at least one resolution path
--    is alive. Cross-reference against REQUIRED_TABLES_BY_TRANSACTION_TYPE
--    in gl_source_join.py for the authoritative OR-path definitions;
--    this is a representative sample, not exhaustive for every type.
-- select distinct TransactionType, count(*) as row_count
-- from TransactionJournal
-- group by TransactionType
-- order by TransactionType;

-- 3. Table row counts, one row per table this pipeline uses — quick way
--    to see at a glance which tables are empty before cross-referencing
--    against REQUIRED_TABLES_BY_TRANSACTION_TYPE and the TransactionType
--    values actually present (query 2 above).
-- select 'invoice_line' as table_name, count(*) as row_count from InvoiceLine
-- union all select 'invoice_line_tax', count(*) from InvoiceLineTax
-- union all select 'payment_line_invoice_line', count(*) from PaymentLineInvoiceLine
-- union all select 'payment_line_invoice', count(*) from PaymentLineInvoice
-- union all select 'credit_memo_line', count(*) from CreditMemoLine
-- union all select 'credit_memo_line_invoice_line', count(*) from CreditMemoLineInvoiceLine
-- union all select 'credit_memo_inv_application', count(*) from CreditMemoInvApplication
-- union all select 'credit_memo_line_tax', count(*) from CreditMemoLineTax
-- union all select 'debit_memo_line', count(*) from DebitMemoLine
-- union all select 'debit_memo_line_tax', count(*) from DebitMemoLineTax
-- union all select 'payment', count(*) from Payment
-- union all select 'refund', count(*) from Refund
-- union all select 'refund_line_payment', count(*) from RefundLinePayment;

-- 4. FAN-OUT CHECK — does a single Invoice/CreditMemo's applied lines ever
--    span multiple Product2Ids? Confirmed acceptable by Dakota either way
--    ("just pull everything in") — informational only, not a blocker.
-- select InvoiceId, count(distinct Product2Id) as distinct_products
-- from InvoiceLine
-- group by InvoiceId
-- having count(distinct Product2Id) > 1;

-- 5. GL_Accounting_Number__c format sanity check.
-- select GL_Accounting_Number__c, length(GL_Accounting_Number__c) as len
-- from GeneralLedgerAccount
-- limit 20;

-- 6. RefundLinePayment / DebitMemoLineTax readiness checks — run these
--    once dataset_id registration / the table itself goes live,
--    respectively, before trusting any output for these TransactionTypes.
-- select count(*) as refund_line_payment_row_count from RefundLinePayment;
-- select count(*) as debit_memo_line_tax_row_count from DebitMemoLineTax;
