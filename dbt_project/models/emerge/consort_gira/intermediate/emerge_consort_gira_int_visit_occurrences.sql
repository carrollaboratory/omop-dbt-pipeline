{{ config(materialized='table') }}

with unioned as (
    select emerge_id, encounter_id, age_at_event,
    from {{ ref('emerge_consort_gira_src_emerge_measurement_ex_release_20260127') }}
    where emerge_id not in (select emerge_id from {{ ref('emerge_consort_gira_lookup_exclusion') }})
    union
    select emerge_id, encounter_id, age_at_event
    from {{ ref('emerge_consort_gira_src_emerge_bmi_ex_release_20260128') }}
    where emerge_id not in (select emerge_id from {{ ref('emerge_consort_gira_lookup_exclusion') }})
    union
    select emerge_id, encounter_id, age_at_event
    from {{ ref('emerge_consort_gira_src_emerge_cpt_ex_release_20260129') }}
    where emerge_id not in (select emerge_id from {{ ref('emerge_consort_gira_lookup_exclusion') }})
    union
    select emerge_id, encounter_id, age_at_event
    from {{ ref('emerge_consort_gira_src_emerge_icd_ex_release_20260129') }}
    where emerge_id not in (select emerge_id from {{ ref('emerge_consort_gira_lookup_exclusion') }})
    
   ),
add_index as (
    select *, ROW_NUMBER() OVER () AS visit_index
    from unioned
    )
  
select *,
    {{ generate_key('combined', 'consort_gira', 'visit_index') }}::integer as "visit_occurrence_id",
    substring(emerge_id, 1, 2) as src_care_site
from add_index
left join (select emerge_id, birth_date from {{ ref('emerge_consort_gira_int_person_persons') }} )
using (emerge_id)