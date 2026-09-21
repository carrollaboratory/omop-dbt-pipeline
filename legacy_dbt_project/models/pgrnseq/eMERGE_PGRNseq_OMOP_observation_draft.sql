{{ config(
    alias='observation'
) }}

SELECT
    "observation_id"::INTEGER AS "observation_id",
    "person_id"::INTEGER AS "person_id",
    "observation_concept_id"::INTEGER AS "observation_concept_id",
    "observation_date"::DATE AS "observation_date",
    "observation_datetime"::DATETIME AS "observation_datetime",
    "observation_type_concept_id"::INTEGER AS "observation_type_concept_id",
    "value_as_number"::FLOAT AS "value_as_number",
    "value_as_string"::TEXT AS "value_as_string",
    "value_as_concept_id"::INTEGER AS "value_as_concept_id",
    "qualifier_concept_id"::INTEGER AS "qualifier_concept_id",
    "unit_concept_id"::INTEGER AS "unit_concept_id",
    "provider_id"::INTEGER AS "provider_id",
    "visit_occurrence_id"::INTEGER AS "visit_occurrence_id",
    "visit_detail_id"::INTEGER AS "visit_detail_id",
    "observation_source_value"::TEXT AS "observation_source_value",
    "observation_source_concept_id"::INTEGER AS "observation_source_concept_id",
    "unit_source_value"::TEXT AS "unit_source_value",
    "qualifier_source_value"::TEXT AS "qualifier_source_value"
FROM read_csv('../../_study_data/emerge_legacy/eMERGE_PGRNseq_OMOP_observation_draft.csv', AUTO_DETECT=FALSE, HEADER=TRUE, nullstr = ["null", "NA", "N/A"], columns={
        'observation_id': 'VARCHAR',
        'person_id': 'VARCHAR',
        'observation_concept_id': 'VARCHAR',
        'observation_date': 'VARCHAR',
        'observation_datetime': 'VARCHAR',
        'observation_type_concept_id': 'VARCHAR',
        'value_as_number': 'VARCHAR',
        'value_as_string': 'VARCHAR',
        'value_as_concept_id': 'VARCHAR',
        'qualifier_concept_id': 'VARCHAR',
        'unit_concept_id': 'VARCHAR',
        'provider_id': 'VARCHAR',
        'visit_occurrence_id': 'VARCHAR',
        'visit_detail_id': 'VARCHAR',
        'observation_source_value': 'VARCHAR',
        'observation_source_concept_id': 'VARCHAR',
        'unit_source_value': 'VARCHAR',
        'qualifier_source_value': 'VARCHAR'
    })