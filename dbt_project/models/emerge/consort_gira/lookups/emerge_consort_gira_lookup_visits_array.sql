{{ config(materialized='table') }}


select
    emerge_id,
    array_agg(distinct encounter_id) as encounter_id_list
from {{ ref('emerge_consort_gira_lookup_visits') }}
group by emerge_id