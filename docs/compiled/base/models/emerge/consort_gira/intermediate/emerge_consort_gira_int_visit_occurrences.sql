

with unioned as (
    select emerge_id, encounter_id, age_at_event,
    from "dbt"."main"."emerge_consort_gira_src_emerge_measurement_ex_release_20260127"
    where emerge_id not in (select emerge_id from "dbt"."main"."emerge_consort_gira_lookup_exclusion") and age_at_event is not null
    union
    select emerge_id, encounter_id, age_at_event
    from "dbt"."main"."emerge_consort_gira_src_emerge_bmi_ex_release_20260128"
    where emerge_id not in (select emerge_id from "dbt"."main"."emerge_consort_gira_lookup_exclusion")
    union
    select emerge_id, encounter_id, age_at_event
    from "dbt"."main"."emerge_consort_gira_src_emerge_cpt_ex_release_20260129"
    where emerge_id not in (select emerge_id from "dbt"."main"."emerge_consort_gira_lookup_exclusion")
    union
    select emerge_id, encounter_id, age_at_event
    from "dbt"."main"."emerge_consort_gira_src_emerge_icd_ex_release_20260129"
    where emerge_id not in (select emerge_id from "dbt"."main"."emerge_consort_gira_lookup_exclusion")
    
   ),
add_index as (
    select *, ROW_NUMBER() OVER () AS visit_index
    from unioned
    )
  
select *,
    CAST(200000000 AS INTEGER) + CAST(CONCAT('6', visit_index) AS INTEGER)::integer as "visit_occurrence_id",
    substring(emerge_id, 1, 2) as src_care_site
from add_index
left join (select emerge_id, birth_date from "dbt"."main"."emerge_consort_gira_int_person_persons" )
using (emerge_id)