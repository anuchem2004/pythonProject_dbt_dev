{{ config(materialized='table', alias='wt_individual_d') }}

with wt_individual_d as (

    SELECT *
--              individual_sk,
--              maint_dt
              --              email_sk,
--              update_dt
--              email_individual_sk,
    FROM {{source('hrst','wt_individual_d')}}
)
SELECT * FROM wt_individual_d