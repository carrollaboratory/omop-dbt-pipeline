{{ config(materialized='table') }}

    select
    *
    from {{ ref('DRUG_STRENGTH') }}