{{ config(materialized='table') }}

    select
    *
    from {{ ref('VOCABULARY') }}