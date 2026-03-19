{{ config(materialized='table') }}
        
    SELECT
    emerge_id,
    age_at_event,
    cpt_code,
    mci.src_concept_id as "cpt_id", -- concept_id for the cpt_code. This could be non-standard. 
    mci.s_concept_id as "s_observation_concept_id",
    mci.s_concept_code as "s_observation_concept_code",
    row_id,
    encounter_id,
    gira_ror,
    src_index,
    FROM {{ ref('emerge_consort_gira_src_emerge_cpt_ex_release_20260129') }} src
    JOIN (SELECT -- JOIN used to drop rows that are not domain 'observation'
          s_concept_id, s_concept_code, src_concept_code, src_concept_id, domain_id
          FROM {{ ref('emerge_consort_gira_lookup_standards') }} 
          WHERE src_table = 'CPT'
          AND domain_id = 'Observation'
          ) AS mci
        ON src.cpt_code = mci.src_concept_code
    where emerge_id not in (select emerge_id from {{ ref('emerge_consort_gira_lookup_exclusion') }})
    and age_at_event is not null --todo
    and domain_id is not null