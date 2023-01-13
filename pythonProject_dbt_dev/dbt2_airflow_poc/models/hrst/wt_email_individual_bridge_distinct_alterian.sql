{{ config (materialized='table', alias='wt_email_individual_bridge_distinct_alterian') }}


WITH wt_email_individual_bridge AS (
	SELECT
		*
	FROM {{ ref('wt_email_individual_bridge') }}

),

wt_email_d AS (
	SELECT
		*
	FROM {{ ref('wt_email_d') }}

),

wt_email_individual_bridge_distinct_alterian AS (
    SELECT
                ib.individual_sk,
                ib.email_sk,
                ib.update_dt
    FROM   wt_email_individual_bridge ib
    JOIN wt_email_d e
      ON e.email_sk = ib.email_sk
--        AND e.email_auth <> 'N'
--        AND e.email_valid <> 'N'
	ORDER BY ib.update_dt DESC

)

SELECT * FROM wt_email_individual_bridge_distinct_alterian
