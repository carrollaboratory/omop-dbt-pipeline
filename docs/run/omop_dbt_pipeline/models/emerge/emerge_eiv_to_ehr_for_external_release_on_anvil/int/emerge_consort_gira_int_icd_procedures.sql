
    

    create  table
      "dbt"."dev_202609_6mo_int"."emerge_consort_gira_int_icd_procedures__dbt_tmp"
  
    
    as (
      
        
SELECT
    emerge_id,
    age_at_event,
    icd_code,
    mci.src_concept_id as icd_id,
    mci.s_concept_id as "s_procedure_concept_id",
    mci.s_concept_code as "s_procedure_concept_code",
    icd_flag,
    row_id,
    encounter_id,
    gira_ror,
    src_index,
FROM "dbt"."dev_202609_6mo_src"."emerge_consort_gira_src_emerge_icd_ex_release_20260129" src
    JOIN (SELECT -- JOIN used to drop rows that are not domain 'Procedure'
          s_concept_id, s_concept_code, src_concept_id, src_concept_code, domain_id
          FROM "dbt"."dev_202609_6mo_lookups"."emerge_consort_gira_lookup_standards" 
          WHERE src_table = 'ICD'
          AND domain_id = 'Procedure'
          AND relationship_id = 'Maps to'
          ) AS mci
        ON src.icd_code = mci.src_concept_code
    where emerge_id not in (select emerge_id from "dbt"."dev_202609_6mo_lookups"."emerge_consort_gira_lookup_exclusion")
    and domain_id is not null
    );
    
  