{{ config(materialized='table', schema = 'omop') }}

    select
    null::integer as "domain_concept_id_1",
    null::integer as "fact_id_1",
    null::integer as "domain_concept_id_2",
    null::integer as "fact_id_2",
    null::integer as "relationship_concept_id"
    