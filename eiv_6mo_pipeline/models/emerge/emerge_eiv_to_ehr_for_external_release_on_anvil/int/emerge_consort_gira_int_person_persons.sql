{{ config(materialized='table') }}

select
    src.emerge_id,
    src.withdrawal_status,
    null_cleaned_yob as year_of_birth,
    make_date(CAST(null_cleaned_yob as integer), 6, 15) as birth_date, 
    src.gender_concept_id,
    coalesce(gender.s_concept_id, '0') as s_gender_concept_id,
    race_concept_id,
    coalesce(race_concept_id,'0') as s_race_concept_id,
    ethnicity_concept_id,
    coalesce(ethnicity_concept_id,'0') as s_ethnicity_concept_id
from (select *, coalesce(CAST(year_of_birth as integer),1970) as null_cleaned_yob from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260401') }} ) src
left join (
    select s_concept_id, src_concept_id
    from {{ ref('emerge_consort_gira_lookup_standards') }}
    where vocabulary_id in ('Gender')
    AND relationship_id = 'Maps to'
) gender
    on src.gender_concept_id = gender.src_concept_id
where src.emerge_id not in (select emerge_id from {{ ref('emerge_consort_gira_lookup_exclusion') }})
