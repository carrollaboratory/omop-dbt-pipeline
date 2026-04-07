


with cleaned_race_ethnicity as (
    select
        emerge_id,
        s_concept_id,
        concept_value as race_ethnicity_concept_id,
        concept_type as original_column_flag,
        domain_id
    from "dbt"."main"."emerge_consort_gira_src_emerge_person_ex_release_20260123"
         unpivot (concept_value for concept_type in (race_concept_id, ethnicity_concept_id)) p
    left join (
        select
            s_concept_id, src_concept_id, s_concept_code, domain_id
        from "dbt"."main"."emerge_consort_gira_lookup_standards"
        where vocabulary_id in ('Race', 'Ethnicity')
        AND relationship_id = 'Maps to'
    ) v
    on p.concept_value = v.src_concept_id
    where concept_value is not null
)

select
    src.emerge_id,
    src.withdrawal_status,
    coalesce(CAST(src.year_of_birth as integer),1970) as year_of_birth, -- Handle null year_of_birth
    make_date(CAST(year_of_birth as integer), 6, 15) as birth_date, 
    src.gender_concept_id,
    coalesce(v.s_concept_id, '0') as s_gender_concept_id,
    -- Aggregate to ensure one row per participant
    MAX(case when cre.domain_id = 'Race' then cre.s_concept_id else '0' end) as s_race_concept_id,
    MAX(case when cre.domain_id = 'Ethnicity' then cre.s_concept_id else '0' end) as s_ethnicity_concept_id
from "dbt"."main"."emerge_consort_gira_src_emerge_person_ex_release_20260123" src
left join cleaned_race_ethnicity cre
    on src.emerge_id = cre.emerge_id
    and cre.s_concept_id is not null
    and cre.domain_id in ('Race', 'Ethnicity')
left join (
    select s_concept_id, src_concept_id
    from "dbt"."main"."emerge_consort_gira_lookup_standards"
    where vocabulary_id in ('Gender')
    AND relationship_id = 'Maps to'
) v
    on src.gender_concept_id = v.src_concept_id
where src.emerge_id not in (select emerge_id from "dbt"."main"."emerge_consort_gira_lookup_exclusion")
group by
    src.emerge_id,
    src.withdrawal_status,
    src.year_of_birth,
    src.gender_concept_id,
    v.s_concept_id