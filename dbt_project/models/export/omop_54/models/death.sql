{{ config(materialized='table') }}

    select
    person_id::integer as "person_id",
    death_date::text as "death_date",
    death_datetime::timestamp as "death_datetime",
    death_type_concept_id::integer as "death_type_concept_id",
    cause_concept_id::integer as "cause_concept_id",
    cause_source_value::text as "cause_source_value",
    cause_source_concept_id::integer as "cause_source_concept_id"
    from {{ ref('omop_54_death') }}
    