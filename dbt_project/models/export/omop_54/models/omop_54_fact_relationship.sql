{{ config(materialized='table') }}

    select
    domain_concept_id_1::integer as "domain_concept_id_1",
    fact_id_1::integer as "fact_id_1",
    domain_concept_id_2::integer as "domain_concept_id_2",
    fact_id_2::integer as "fact_id_2",
    relationship_concept_id::integer as "relationship_concept_id"
    from {{ ref('omop_54_fact_relationship') }}
    