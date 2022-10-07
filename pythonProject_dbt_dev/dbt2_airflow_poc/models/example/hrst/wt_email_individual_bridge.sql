

{{ config(materialized='table', alias='wt_email_individual_bridge') }}

with wt_email_individual_bridge as (

    SELECT  *
--              individual_sk,
--              email_sk,
--              update_dt
--              email_individual_sk,
    FROM {{source('hrst','wt_email_individual_bridge')}}
)
SELECT * FROM wt_email_individual_bridge



