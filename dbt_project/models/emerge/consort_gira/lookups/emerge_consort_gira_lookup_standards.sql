{{ config(materialized='table') }}
        
    
    with 
    
    filtered_concept as (
        select 
        distinct concept_id_1 as src_concept_id, 
        concept_code_1 as src_concept_code,
        src_table,
        vocabulary_id,
        domain_id
        from {{ ref ('emerge_consort_gira_lookup_concepts') }} c
        where (concept_id_1 != '4245997' or concept_id_1 is null) -- add the bmi concept standard manually.
    ),
    
    ranked_relationships as (
        select *,
        row_number() over (partition by concept_id_1 order by concept_id_2) as rn
        from {{ ref('CONCEPT_RELATIONSHIP') }}
        where relationship_id = 'Maps to'
        ),
    
    ranked_jcr as (
        select
        distinct src_concept_id,
        src_concept_code,
        src_table,
        vocabulary_id,
        domain_id,
        concept_id_2,
        valid_end_date as "src_valid_end_date"
        from filtered_concept fc
        left join ranked_relationships cr
        on fc.src_concept_id = cr.concept_id_1 and cr.rn = 1 -- TODO analysis: get only the first mapping with the 'Maps to' relationship.
    )
    
    select    
    '4245997' as "src_concept_id", 
    'Body mass index' as "src_concept_code",
    'BMI' as "src_table",
    'SNOMED' as "vocabulary_id",
    'Measurement' as "domain_id",
    '3038553' as "s_concept_id",
    'Body mass index (BMI) [Ratio]' as "s_concept_code",
    'LOINC' as 's_vocabulary_id'
    
    union 
    
    select
    distinct src_concept_id, 
    src_concept_code,
    src_table,
    jcr.vocabulary_id,
    jcr.domain_id,
    case when cast(src_valid_end_date as string) = '20991231' then base_concept.concept_id 
    else 0 end as "s_concept_id", -- Null if the src doesn't join to CR. 0 if the src or standard are not valid
    case when cast(src_valid_end_date as string) = '20991231' then base_concept.concept_code 
    else null end as "s_concept_code",
    base_concept.vocabulary_id as 's_vocabulary_id'
    from ranked_jcr as jcr
    left join (select
               vocabulary_id,
               case when cast(valid_end_date as string) = '20991231' then concept_id
                    else 0 end as "concept_id",
               case when cast(valid_end_date as string) = '20991231' then concept_code
                    else null end as "concept_code"
               from {{ ref('CONCEPT') }}
               ) as base_concept
    on jcr.concept_id_2 = base_concept.concept_id

    