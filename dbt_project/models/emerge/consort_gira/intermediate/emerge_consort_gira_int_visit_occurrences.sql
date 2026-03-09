{{ config(materialized='table') }}

with unioned as (
    select emerge_id, encounter_id, age_at_event, src_index + 3000000 as visit_occurrence_id
    from {{ ref('emerge_consort_gira_src_emerge_measurement_ex_release_20260127') }}
    union
    select emerge_id, encounter_id, age_at_event, src_index + 3000000 as visit_occurrence_id
    from {{ ref('emerge_consort_gira_src_emerge_bmi_ex_release_20260128') }}
    union
    select emerge_id, encounter_id, age_at_event, src_index + 3000000 as visit_occurrence_id
    from {{ ref('emerge_consort_gira_src_emerge_cpt_ex_release_20260129') }}
    union
    select emerge_id, encounter_id, age_at_event, src_index + 3000000 as visit_occurrence_id
    from {{ ref('emerge_consort_gira_src_emerge_icd_ex_release_20260129') }}
    
   )
  
select *
from unioned
left join (select emerge_id, birth_date from {{ ref('emerge_consort_gira_int_person_persons') }} )
using (emerge_id)