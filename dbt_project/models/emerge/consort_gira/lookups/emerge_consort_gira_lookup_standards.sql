{{ config(materialized='table') }}
        
    
    with 
    
    filtered_concept as (
        select 
        distinct c.concept_id_1 as src_concept_id, 
        c.concept_code_1 as src_concept_code,
        src_table,
        relationship_id,
        concept_id_2
        from {{ ref ('emerge_consort_gira_lookup_concepts') }} c
        left join (select *,
            from {{ ref('CONCEPT_RELATIONSHIP') }} cr
            where relationship_id in ('Maps to', 'Maps to value')       
           ) using (concept_id_1)
        where (concept_id_1 != '4245997' or concept_id_1 is null) -- add the bmi concept standard manually.
    )
    
    select    
    '4245997' as "src_concept_id", 
    'Body mass index' as "src_concept_code",
    'BMI' as "src_table",
    '3038553' as "s_concept_id",
    'Body mass index (BMI) [Ratio]' as "s_concept_code",
    'Maps to' as "relationship_id",
    'Measurement' as "domain_id"
    
    union 
    
    select
    distinct src_concept_id, 
    src_concept_code,
    src_table,
    coalesce(c.concept_id, '0') as "s_concept_id",
    coalesce(c.concept_code, '0') as "s_concept_code",
    relationship_id,
    c.domain_id
    from filtered_concept fc
    left join (select
               concept_id,
               concept_code,
               domain_id
               from {{ ref('CONCEPT') }}
               ) as c
    on fc.concept_id_2 = c.concept_id

    