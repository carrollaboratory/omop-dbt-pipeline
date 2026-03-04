{{ config(materialized='table') }}
        
    
    with 
    
    filtered_concept as (
        select 
        distinct concept_id as src_concept_id, 
        concept_code as src_concept_code,
        src_table,
        vocabulary_id,
        domain_id
        from {{ ref ('emerge_consort_gira_lookup_concept') }} c
    ),
    
    jcr as (
        select  
        distinct src_concept_id, 
        src_concept_code,
        src_table,
        vocabulary_id,
        domain_id,
        concept_id_2 as "s_concept_id",
        from filtered_concept fc
        left join (select * 
                   from {{ ref('CONCEPT_RELATIONSHIP') }}
                   where relationship_id = 'Maps to' and invalid_reason is NULL
                  ) cr
        on fc.src_concept_id = cr.concept_id_1
    )
    
    select    
    distinct src_concept_id, 
    src_concept_code,
    src_table,
    jcr.vocabulary_id,
    jcr.domain_id,
    s_concept_id,
    base_concept.concept_code as "s_concept_code",
    base_concept.standard_concept as 's_standard_concept',
    base_concept.vocabulary_id as 's_vocabulary_id'
    from jcr
    left join main_main.CONCEPT base_concept
    on jcr.s_concept_id = base_concept.concept_id

    