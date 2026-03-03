{{ config(materialized='table') }}

    select
    *
    from {{ ref('CONCEPT_RELATIONSHIP') }}