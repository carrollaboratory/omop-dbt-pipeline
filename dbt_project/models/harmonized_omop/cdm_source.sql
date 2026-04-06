{{ config(materialized='table', schema = 'omop') }}

select
'eMERGE Consortium GIRA eIV' as "cdm_source_name",
'eIV' as "cdm_source_abbreviation",
'eMERGE' as "cdm_holder",
null as "source_description",
null as "source_documentation_reference",
null as"cdm_etl_refererence",
CAST('2026-01-23' as DATE) as "source_release_date",
CAST('2026-03-13' as DATE) as "cdm_release_date",
'v5.4' as "cdm_version",
705800 as "cdm_version_concept_id",
'v5.0 27-FEB-26' as "vocabulary_version"
FROM {{ ref('hidden') }}