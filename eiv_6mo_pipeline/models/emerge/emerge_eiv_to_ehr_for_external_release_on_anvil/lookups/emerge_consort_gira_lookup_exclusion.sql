{{ config(materialized='table') }}
with base as (
    select distinct emerge_id
    from {{ ref('emerge_consort_gira_src_emerge_measurement_ex_release_20260922') }}
    union
    select distinct emerge_id
    from {{ ref('emerge_consort_gira_src_emerge_bmi_ex_release_20260922') }}
    union
    select distinct emerge_id
    from {{ ref('emerge_consort_gira_src_emerge_cpt_ex_release_20260922') }}
    union
    select distinct emerge_id
    from {{ ref('emerge_consort_gira_src_emerge_icd_ex_release_20260922') }}
)
select emerge_id from base
where emerge_id not in (select distinct emerge_id from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260401') }})
and emerge_id not in (select distinct emerge_id from {{ ref('emerge_consort_gira_src_emerge_6_month_consent_ds_20260731') }} where consent = '1')
