{{ config(materialized='table') }}

    select
    metadata_concept_id::integer as "metadata_concept_id",
    metadata_type_concept_id::integer as "metadata_type_concept_id",
    name::text as "name",
    value_as_string::text as "value_as_string",
    value_as_concept_id::integer as "value_as_concept_id",
    metadata_date::text as "metadata_date",
    metadata_datetime::timestamp as "metadata_datetime"
    from {{ ref('omop_54_metadata') }}
    