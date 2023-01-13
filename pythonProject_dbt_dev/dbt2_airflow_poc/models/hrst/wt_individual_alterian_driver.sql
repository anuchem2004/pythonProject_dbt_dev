{{ config(materialized='table',alias = 'wt_individual_alterian_driver') }}

with wt_individual_d as (
    SELECT     i.individual_sk,i.maint_dt,
                i.curr_rec_ind
    FROM       {{ref('wt_individual_d')}} i
    INNER JOIN {{source('hrst_stg','source_last_send_alterian')}} slsa
    ON         slsa.source = 'wt_individual_d'
    WHERE      i.maint_dt > slsa.updatedate
    AND 	   i.curr_rec_ind = 1
),


wt_individual_flags_d as (
    SELECT     indf.individual_sk
    FROM       {{ref('wt_individual_flags_d')}} indf
    INNER JOIN {{source('hrst_stg','source_last_send_alterian')}} slsa
    ON         slsa.source = 'wt_individual_d'
    WHERE      indf.update_dt > slsa.updatedate
  ),

wt_email_individual_bridge as(

    SELECT     eib.individual_sk
    FROM       {{ref('wt_email_individual_bridge')}} eib
    INNER JOIN {{source('hrst_stg','source_last_send_alterian')}} slsa
    ON         slsa.source = 'wt_individual_d'
    WHERE      eib.update_dt > slsa.updatedate
)

SELECT individual_sk from wt_email_individual_bridge
UNION
select individual_sk from wt_individual_flags_d
UNION
SELECT individual_sk from wt_individual_d













