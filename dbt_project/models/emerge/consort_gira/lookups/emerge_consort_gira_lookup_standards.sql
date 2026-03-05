{{ config(materialized='table') }}
        
    
    with 
    
    filtered_concept as (
        select 
        distinct concept_id as src_concept_id, 
        concept_code as src_concept_code,
        src_table,
        vocabulary_id,
        domain_id
        from {{ ref ('emerge_consort_gira_lookup_concepts') }} c
    ),
    
    jcr as (
        select  
        distinct src_concept_id, 
        src_concept_code,
        src_table,
        vocabulary_id,
        domain_id,
        concept_id_2
        from filtered_concept fc
        left join (select * -- TODO make sure there are not more than one relationship 'Maps to'. Check for duplicates.
                   from {{ ref('CONCEPT_RELATIONSHIP') }}
                   where relationship_id = 'Maps to'  -- and invalid_reason is NULL
                  ) cr
        on fc.src_concept_id = cr.concept_id_1
    )
    
    select    
    distinct src_concept_id, 
    src_concept_code,
    src_table,
    jcr.vocabulary_id,
    jcr.domain_id,
      case 
      when concept_id_2 is not null and valid_end_date = '20991231' then concept_id_2
      -- A standard concept exists and is valid
      when valid_end_date != '20991231' then 0
      -- A standard concept exists but it is invalid
      else null
      -- The Src-cid didn't join to CONCEPT.
      -- The Src-cid didn't join to CONCEPT_RELATIONSHIP.
    end as "s_concept_id",
      case 
      when concept_id_2 is not null and valid_end_date = '20991231' then base_concept.concept_code
      -- A standard concept exists and is valid
      else null      
      -- A standard concept exists but it is invalid      
      -- The Src-cid didn't join to CONCEPT.
      -- The Src-cid didn't join to CONCEPT_RELATIONSHIP.
    end as "s_concept_code",
    base_concept.vocabulary_id as 's_vocabulary_id'
    from jcr
    left join (select cast(valid_end_date as string) as valid_end_date, concept_id, concept_code , vocabulary_id
               from main_main.CONCEPT
               ) as base_concept
    on jcr.concept_id_2 = base_concept.concept_id

    