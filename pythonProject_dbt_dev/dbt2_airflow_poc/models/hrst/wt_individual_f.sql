{{ config(materialized='table', alias='wt_individual_f') }}

WITH wt_individual_f AS (
SELECT i.individual_sk,
       CASE
         WHEN i.household_id = 2 THEN 2 + i.individual_sk + 6000000000000
         ELSE i.household_id
       end                            AS household_id,
       CASE
         WHEN i.address_id = 2 THEN 2 + i.individual_sk + 6000000000000
         ELSE i.address_id
       end                            AS address_id,
       i.address_data_src,
       CASE
         WHEN i.business_name = i.name_description THEN NULL
         ELSE i.pre_name
       end                            AS pre_name,
       CASE
         WHEN i.business_name = i.name_description THEN NULL
         ELSE i.first_name
       end                            AS first_name,
       CASE
         WHEN i.business_name = i.name_description THEN NULL
         ELSE i.middle_name
       end                            AS middle_name,
       CASE
         WHEN i.business_name = i.name_description THEN NULL
         ELSE i.last_name
       end                            AS last_name,
       CASE
         WHEN i.business_name = i.name_description THEN NULL
         ELSE i.suffix
       end                            AS suffix,
       i.name_type_cd,
       CASE
         WHEN i.name_type_cd = '1' THEN 'First Initial'
         WHEN i.name_type_cd = '2' THEN 'Full First Name'
         WHEN i.name_type_cd = '4' THEN 'Last Name Only'
         WHEN i.name_type_cd = '5' THEN 'Current Resindent'
         WHEN i.name_type_cd = ' ' THEN 'Business Only'
         ELSE 'default'
       end                            AS name_type_name,
         i.original_full_name,
       CASE
         WHEN  i.business_name = '' and i.name_description = '' THEN i.original_full_name
         WHEN  i.business_name is null and i.name_description is null THEN i.original_full_name
         WHEN i.business_name = i.name_description THEN NULL
         ELSE i.name_description
       end                          AS full_name,
       i.original_company_name,
         CASE
         WHEN  i.business_name = '' and i.name_description = '' THEN i.original_company_name
         WHEN  i.business_name is null and i.name_description is null THEN i.original_company_name
         ELSE i.business_name
       end                          AS business_name,
       i.address_line_1,
       i.address_line_2,
       i.city_name,
       i.state_province_region,
       i.postal_code,
       i.zip_plus4,
       i.scf_fsa,
       niel.county_size_cd                 AS nielsen_cd,
       CASE
         WHEN niel.county_size_cd = '6' THEN 'Highly Urbanized'
         WHEN niel.county_size_cd = '7' THEN 'Relatively Urbanized'
         WHEN niel.county_size_cd = '8' THEN 'Relatively Rural'
         WHEN niel.county_size_cd = '9' THEN 'Very Rural'
         ELSE 'default'
       end                            AS nielsen_name,
       niel.dma_cd                         AS dma_cd,
       niel.dma_name                       AS dma_name,
       CASE
         WHEN i.state_province_region in ('AA', 'AE', 'AP') THEN 'A'
         WHEN i.state_province_region in ('AS','FM','GU','MP','PW','PR','VI') AND i.country = 'US' THEN 'P'
         WHEN i.state_province_region in ('AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL',
                                         'IN','IA','KS','KY','LA','ME','MH','MD','MA','MI','MN','MS','MO','MT',
                                         'NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
                                         'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY') AND i.country = 'US' THEN 'U'
         WHEN i.state_province_region in ('AB','BC','MB','NB','NL','NS','NT','NU','ON','PE','QC','SK','YT') THEN 'C'
         WHEN i.country not in ('US', 'CANADA') THEN 'F'
         ELSE ''
       end                            AS country_typ_cd,
       CASE
         WHEN i.state_province_region in ('AA', 'AE', 'AP') THEN 'APO/FPO'
         WHEN i.state_province_region in ('AS','FM','GU','MP','PW','PR','VI') AND i.country = 'US' THEN 'US Possession'
         WHEN i.state_province_region in ('AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL',
                                         'IN','IA','KS','KY','LA','ME','MH','MD','MA','MI','MN','MS','MO','MT',
                                         'NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
                                         'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY') AND i.country = 'US' THEN 'US'
         WHEN i.state_province_region in ('AB','BC','MB','NB','NL','NS','NT','NU','ON','PE','QC','SK','YT') THEN 'Canadian'
         WHEN i.country not in ('US', 'CANADA') THEN 'Foreign'
         ELSE 'default'
       end                            AS country_typ_name,
        CASE
         WHEN i.part_CD in ('U', 'C') AND (i.hygiene_change_flag IN ('D', 'U') OR i.deliverability_score = 8) THEN  'U'
         WHEN i.deliverability_score = 6 AND i.part_CD = 'F' THEN  'U'
         else 'D'
        end AS    delivery_status_cd,
        CASE
         WHEN i.part_CD in ('U', 'C') AND (i.hygiene_change_flag IN ('D', 'U') OR i.deliverability_score = 8) THEN  'Undeliverable'
         WHEN i.deliverability_score = 6 AND i.part_CD = 'F' THEN  'Undeliverable'
         else 'Deliverable'
        end                            AS delivery_status_name,
       i.delivery_point,
       i.carrier_route,
       i.deliverability_score,
       i.dsf2_record_type,
       i.coa_dt,
       i.coa_data_source,
       i.ncoa_run_dt,
       i.ncoa_move_type,
       i.ncoa_category,
       i.ncoa_footnote,
       i.ncoa_move_eff_dt,
       i.deceased_flag,
       i.pander_flag,
       i.vulgar_address_flag,
       i.vulgar_name_flag,
       indf.best_gender               AS best_gender,
       i.prison_flag,
       e.email_address,
       i.country,
--           Date_format(Now(), '%Y-%m-%d') AS load_dt


FROM   {{ref('wt_individual_d')}} i
       INNER JOIN {{ref('wt_individual_alterian_driver')}} iad
               ON i.individual_sk = iad.individual_sk
       LEFT OUTER JOIN {{ref('wt_email_individual_bridge_distinct_alterian')}} eb
                    ON i.individual_sk = eb.individual_sk
       LEFT OUTER JOIN {{ref('wt_email_d')}} e
                    ON eb.email_sk = e.email_sk
       LEFT OUTER JOIN {{ref('wt_individual_flags_d')}} indf
                    ON i.individual_sk = indf.individual_sk
       LEFT OUTER JOIN {{ref('wt_nielsen_dma_d')}} niel
                    ON i.postal_code = niel.zip_cd
                    AND i.country = 'US'

WHERE i.curr_rec_ind = 1
)
SELECT * FROM wt_individual_f



