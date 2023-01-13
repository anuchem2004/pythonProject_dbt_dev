{{ config(materialized='table', alias='wt_individual_flags_d') }}

with wt_individual_flags_d as (

    SELECT  *
--              individual_sk,
----              email_sk,
--              update_dt
--              email_individual_sk,
    FROM {{source('hrst','wt_individual_flags_d')}}
)
SELECT * FROM wt_individual_flags_d