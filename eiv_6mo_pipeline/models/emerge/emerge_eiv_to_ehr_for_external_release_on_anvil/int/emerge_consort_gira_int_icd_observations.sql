{{ config(materialized='table') }}
        
SELECT
    emerge_id,
    age_at_event,
    icd_code,
    mci.src_concept_id as icd_id,
    mci.s_concept_id as "s_observation_concept_id",
    mci.s_concept_code as "s_observation_concept_code",
    vas.value_as_concept_id,
    vas.value_as_concept_code,
    icd_flag,
    row_id,
    encounter_id,
    gira_ror,
    src_index,
FROM {{ ref('emerge_consort_gira_src_emerge_icd_ex_release_20260129') }} src
    JOIN (SELECT -- JOIN used to drop rows that are not domain 'Observation'
          s_concept_id, s_concept_code, src_concept_id, src_concept_code, domain_id
          FROM {{ ref('emerge_consort_gira_lookup_standards') }} 
          WHERE src_table = 'ICD'
          AND domain_id = 'Observation'
          and relationship_id = 'Maps to'
          ) AS mci
        ON src.icd_code = mci.src_concept_code
     left join (select -- JOIN used to drop rows that are not domain 'Measurement'
      src_concept_code, s_concept_id as "value_as_concept_id" , s_concept_code as "value_as_concept_code"
      from {{ ref('emerge_consort_gira_lookup_standards') }} 
      where src_table = 'ICD'
      and relationship_id = 'Maps to value'
      ) as vas
        ON src.icd_code = vas.src_concept_code
    where emerge_id not in (select emerge_id from {{ ref('emerge_consort_gira_lookup_exclusion') }})
    and domain_id is not null