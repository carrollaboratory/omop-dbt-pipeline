{{ config(materialized='table') }}

    with 
    all_site_ids as (
        select substring(emerge_id, 1, 2) as "site_id" 
        from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
        where emerge_id not in (select emerge_id from {{ ref('emerge_consort_gira_lookup_exclusion') }})
    union
        select substring(emerge_id, 1, 2) as "site_id" 
        from {{ ref('emerge_consort_gira_src_emerge_measurement_ex_release_20260127') }}
        where emerge_id not in (select emerge_id from {{ ref('emerge_consort_gira_lookup_exclusion') }})
    union
        select substring(emerge_id, 1, 2) as "site_id" 
        from {{ ref('emerge_consort_gira_src_emerge_bmi_ex_release_20260128') }}
        where emerge_id not in (select emerge_id from {{ ref('emerge_consort_gira_lookup_exclusion') }})
    union
        select substring(emerge_id, 1, 2) as "site_id" 
        from {{ ref('emerge_consort_gira_src_emerge_cpt_ex_release_20260129') }}
        where emerge_id not in (select emerge_id from {{ ref('emerge_consort_gira_lookup_exclusion') }})
    union
        select substring(emerge_id, 1, 2) as "site_id" 
        from {{ ref('emerge_consort_gira_src_emerge_icd_ex_release_20260129') }}
        where emerge_id not in (select emerge_id from {{ ref('emerge_consort_gira_lookup_exclusion') }})
)

select
    all_site_ids.site_id,
    s.dbgap_site_name as "site_name"
from all_site_ids
left join main.care_site_seed s
    on all_site_ids.site_id = s.dbgap_site_id