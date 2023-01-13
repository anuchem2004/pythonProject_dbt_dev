
{{ config(materialized='table', alias='create_cancel_orders_extract_file') }}
create  table
    "dev"."hrst"."create_cancel_orders_extract_file"

as
with create_cancel_orders_extract_file as (

SELECT
    mo.individual_sk,
    mo.magazine_abbr,
    mo.account_nbr,
    mo.order_nbr,
    mo.order_dt,
    cs.credit_status_name,
    oet.order_entry_type_name,
    mo.cancel_dt,
    cr.cancel_reason_name,
    ct.cancel_type_name,
    mi.issue_desc AS cancel_issue_desc,
    mo.document_key,
    mo.bill_key,
    mo.order_amt,
    mo.agency_gross_amt,
    os.order_source_name,
    gst.gift_set_type_name,
    gt.gift_type_name,
    m.medium_name,
    rt.renewal_type_name,
    a.agency_name,
    mc.magazine_category_name,
    pm.payment_method_name,
    pt.payment_type_name,
    ps.paid_status_name
FROM
     ${database}_stg.work_cancel_magazine_order_stg_clmn mo
JOIN wt_credit_status_d_clmn cs ON cs.credit_status_sk = mo.credit_status_sk
JOIN wt_order_entry_type_d_clmn oet ON oet.order_entry_type_sk = mo.order_entry_type_sk
JOIN wt_cancel_reason_d_clmn cr ON cr.cancel_reason_sk = mo.cancel_reason_sk
JOIN wt_cancel_type_d_clmn ct ON ct.cancel_type_sk = mo.cancel_type_sk
JOIN ${database}_stg.work_cancel_magazine_order_issue_desc mi ON mo.account_nbr = mi.account_nbr
	AND mo.magazine_sk = mi.magazine_sk
	AND mo.order_nbr = mi.order_nbr
	AND mo.gift_set_type_sk = mi.gift_set_type_sk
JOIN wt_order_source_d_clmn os ON os.order_source_sk = mo.order_source_sk
JOIN wt_gift_set_type_d_clmn gst ON gst.gift_set_type_sk = mo.gift_set_type_sk
JOIN wt_gift_type_d_clmn gt ON gt.gift_type_sk = mo.gift_type_sk
JOIN wt_medium_d_clmn m ON m.medium_sk = mo.medium_sk
JOIN wt_renewal_type_d_clmn rt ON rt.renewal_type_sk = mo.renewal_type_sk
JOIN wt_agency_d_clmn a ON a.agency_sk = mo.agency_sk AND a.curr_rec_ind = 1
JOIN wt_magazine_category_d_clmn mc ON mc.magazine_category_sk = mo.magazine_category_sk
JOIN wt_payment_method_d_clmn pm ON pm.payment_method_sk = mo.payment_method_sk
JOIN wt_payment_type_d_clmn pt ON pt.payment_type_sk = mo.payment_type_sk
JOIN wt_paid_status_d_clmn ps ON ps.paid_status_sk = mo.paid_status_sk

);
SELECT * FROM create_cancel_orders_extract_file;