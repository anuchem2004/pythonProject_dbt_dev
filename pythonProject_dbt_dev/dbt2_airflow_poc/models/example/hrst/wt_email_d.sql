
{{ config(materialized='table', alias='wt_email_d') }}

with wt_email_d as (

    SELECT *
--            email_sk,
--            email_auth,
--           email_valid

    FROM {{source('hrst','wt_email_d')}}
    )

SELECT * FROM wt_email_d


