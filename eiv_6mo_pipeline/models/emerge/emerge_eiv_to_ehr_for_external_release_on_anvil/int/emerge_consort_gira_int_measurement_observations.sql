{{ config(materialized='table') }}
     
SELECT
    emerge_id,
    age_at_event,
    measurement_concept_id,
    mci.s_concept_id as "s_observation_concept_id",
    mci.s_concept_code as "s_observation_concept_code",
    value_as_number,
    value_as_text,
    case
      when try_cast(range_low as float) is not null
      then try_cast(range_low as float)
      else null -- TODO What to do when not a float. Example: >=40 Not sure why characterization report didn't pick this up
    end as range_low,    
    case
      when try_cast(range_high as float) is not null
      then try_cast(range_high as float)
      else null -- TODO What to do when not a float. Example: >=40 Not sure why characterization report didn't pick this up
    end as range_high,
    range_flag,
    uci.src_concept_id as "unit_concept_id",
    unit_concept_as_text,
    uci.s_concept_id as "s_unit_concept_id",
    uci.s_concept_code as "s_unit_concept_code",
    vas.value_as_concept_id,
    vas.value_as_concept_code,
    row_id,
    encounter_id,
    gira_ror,
    src_index,
FROM {{ ref('emerge_consort_gira_src_emerge_measurement_ex_release_20260127') }} src
JOIN (SELECT -- JOIN used to drop rows that are not domain 'Observation'
      s_concept_id, s_concept_code, src_concept_id, domain_id
      FROM {{ ref('emerge_consort_gira_lookup_standards') }} 
      WHERE src_table = 'M'
      AND domain_id = 'Observation'
      and relationship_id = 'Maps to value'
      ) AS mci
    ON src.measurement_concept_id = mci.src_concept_id
LEFT JOIN (SELECT
      s_concept_id, s_concept_code, src_concept_id
      FROM {{ ref('emerge_consort_gira_lookup_standards') }} 
      WHERE src_table = 'M'
      AND domain_id != 'Measurement'
      ) AS uci
    ON src.unit_concept_id = uci.src_concept_id
left join (select -- JOIN used to drop rows that are not domain 'Measurement'
      src_concept_id, s_concept_id as "value_as_concept_id" , s_concept_code as "value_as_concept_code"
      from {{ ref('emerge_consort_gira_lookup_standards') }} 
      where src_table = 'M'
      and relationship_id = 'Maps to value'
      ) as vas
      on src.measurement_concept_id = vas.src_concept_id
where emerge_id not in (select emerge_id from {{ ref('emerge_consort_gira_lookup_exclusion') }})
and domain_id is not null
