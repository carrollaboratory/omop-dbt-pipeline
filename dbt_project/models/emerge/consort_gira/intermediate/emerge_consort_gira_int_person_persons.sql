{{ config(materialized='table') }}


with cleaned_race_ethnicity as (
    select
        emerge_id,
        s_concept_id,
        concept_value as "race_ethnicity_concept_id",
        concept_type as "original_column_flag",
        domain_id
    from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }}
         unpivot (concept_value for concept_type in (race_concept_id, ethnicity_concept_id)) p
    left join (
        select
            s_concept_id, src_concept_id, s_concept_code, domain_id
        from {{ ref('emerge_consort_gira_lookup_standards') }}
        where vocabulary_id in ('Race','Ethnicity')
    ) v
    on p.concept_value = v.src_concept_id
    where concept_value is not null
)

select
    src.row_id,
    src.emerge_id,
    src.withdrawal_status,
    src.year_of_birth,
    case when year_of_birth is not null
         then make_date(CAST(year_of_birth as integer), 6, 15)
         else make_date(1970, 6, 15)
    end as birth_date, --TODO What to do when year_of_birth is null?
    src.gender_concept_id,
    max(case when cre.domain_id = 'Race' then cre.s_concept_id end) as race_concept_id,
    max(case when cre.domain_id = 'Ethnicity' then cre.s_concept_id end) as ethnicity_concept_id,
    src_index,
from {{ ref('emerge_consort_gira_src_emerge_person_ex_release_20260123') }} src
left join cleaned_race_ethnicity cre
    on src.emerge_id = cre.emerge_id
    and cre.s_concept_id is not null
    and cre.domain_id in ('Race','Ethnicity')
group by src.row_id, src.emerge_id, src.withdrawal_status, src.year_of_birth, src.gender_concept_id
