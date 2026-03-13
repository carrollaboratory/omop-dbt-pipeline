{{ config(materialized='table', schema = 'omop') }}

select
'OMOP 5.4' as "cdm_source_name",
'OMOP 5.4' as "cdm_source_abbreviation",
'OHDSI' as "cdm_holder",
null as "source_description",
null as "source_domumentation_reference",
null as"cdm_etl_refererence",
CAST('2026-03-13' as DATE) as "source_release_date",
CAST('2026-03-13' as DATE) as "cdm_release_date",
'5.4' as "cdm_version",
1111 as "cdm_version_concept_id",
'202603' as "vocabulary_version"