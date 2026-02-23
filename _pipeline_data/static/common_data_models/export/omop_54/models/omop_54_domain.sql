{{ config(materialized='table') }}

    select
    domain_id::text as "domain_id",
    domain_name::text as "domain_name",
    domain_concept_id::integer as "domain_concept_id"
    from {{ ref('omop_54_domain') }}
    