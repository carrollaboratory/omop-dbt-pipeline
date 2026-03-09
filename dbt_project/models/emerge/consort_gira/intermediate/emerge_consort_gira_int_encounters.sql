{{ config(materialized='table') }}

    select
    emerge_id,
    row_id,
    encounter_id
    from {{ ref('emerge_consort_gira_src_emerge_cpt_ex_release_20260129') }} 
    
    union all
    
    select
    emerge_id,
    row_id,
    encounter_id
    from {{ ref('emerge_consort_gira_src_emerge_bmi_ex_release_20260128') }}
    
    union all
    
    select
    emerge_id,
    row_id,
    encounter_id
    from {{ ref('emerge_consort_gira_src_emerge_icd_ex_release_20260128') }}
    
    union all
    
    select
    emerge_id,
    row_id,
    encounter_id
    from {{ ref('emerge_consort_gira_src_emerge_measurement_ex_release_20260127') }}
    
    SELECT DISTINCT ON (encounter_id) emerge_id, value
    ORDER BY encounter_id, value DESC
