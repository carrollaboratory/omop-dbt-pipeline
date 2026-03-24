{{ config(materialized='table', schema = 'omop') }}

    select
    *
    from {{ ref('CONCEPT') }}