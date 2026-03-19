{{ config(materialized='table') }}
        
select
emerge_id,
age_at_event,
cpt_code,
mci.src_concept_id as "cpt_id", -- concept_id for the cpt_code. This could be non-standard. 
mci.s_concept_id as "s_measurement_concept_id",
mci.s_concept_code as "s_measurement_concept_code",
vas.value_as_concept_id,
vas.value_as_concept_code,
row_id,
encounter_id,
gira_ror,
src_index,
from {{ ref('emerge_consort_gira_src_emerge_cpt_ex_release_20260129') }} src
join (select -- JOIN used to drop rows that are not domain 'Measurement'
      s_concept_id, s_concept_code, src_concept_code, src_concept_id, domain_id
      from {{ ref('emerge_consort_gira_lookup_standards') }} 
      where src_table = 'CPT'
      and relationship_id = 'Maps to'
      and domain_id = 'Measurement'
      ) as mci
    on src.cpt_code = mci.src_concept_code
left join (select -- JOIN used to drop rows that are not domain 'Measurement'
  src_concept_code, s_concept_id as "value_as_concept_id" , s_concept_code as "value_as_concept_code"
  from {{ ref('emerge_consort_gira_lookup_standards') }} 
  where src_table = 'CPT'
  and relationship_id = 'Maps to value'
  ) as vas
  on src.cpt_code = vas.src_concept_code
where emerge_id not in (select emerge_id from {{ ref('emerge_consort_gira_lookup_exclusion') }})
and domain_id is not null