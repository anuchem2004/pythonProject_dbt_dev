
{{ config(materialized='table', alias='wt_nielsen_dma_d') }}

with wt_nielsen_dma_d as (

         SELECT *
--              email_individual_sk,
         FROM {{source('hrst','wt_nielsen_dma_d')}}
)
SELECT * FROM wt_nielsen_dma_d


