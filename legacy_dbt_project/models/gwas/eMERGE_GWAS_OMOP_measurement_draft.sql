{{ config(
    alias='measurement'
) }}

SELECT
    "measurement_id"::INTEGER AS "measurement_id",
    "person_id"::INTEGER AS "person_id",
    "measurement_concept_id"::INTEGER AS "measurement_concept_id",
    "measurement_date"::DATE AS "measurement_date",
    "measurement_datetime"::DATETIME AS "measurement_datetime",
    "measurement_time"::TEXT AS "measurement_time",
    "measurement_type_concept_id"::INTEGER AS "measurement_type_concept_id",
    "operator_concept_id"::INTEGER AS "operator_concept_id",
    "value_as_number"::FLOAT AS "value_as_number",
    "value_as_concept_id"::INTEGER AS "value_as_concept_id",
    "unit_concept_id"::INTEGER AS "unit_concept_id",
    "range_low"::FLOAT AS "range_low",
    "range_high"::FLOAT AS "range_high",
    "provider_id"::INTEGER AS "provider_id",
    "visit_occurrence_id"::INTEGER AS "visit_occurrence_id",
    "visit_detail_id"::INTEGER AS "visit_detail_id",
    "measurement_source_value"::TEXT AS "measurement_source_value",
    "measurement_source_concept_id"::INTEGER AS "measurement_source_concept_id",
    "unit_source_value"::TEXT AS "unit_source_value",
    "value_source_value"::TEXT AS "value_source_value"
FROM read_csv('../../_study_data/emerge_legacy/eMERGE_GWAS_OMOP_measurement_draft.csv', AUTO_DETECT=FALSE, HEADER=TRUE, nullstr = ["null", "NA", "N/A"], columns={
        'measurement_id': 'VARCHAR',
        'person_id': 'VARCHAR',
        'measurement_concept_id': 'VARCHAR',
        'measurement_date': 'VARCHAR',
        'measurement_datetime': 'VARCHAR',
        'measurement_time': 'VARCHAR',
        'measurement_type_concept_id': 'VARCHAR',
        'operator_concept_id': 'VARCHAR',
        'value_as_number': 'VARCHAR',
        'value_as_concept_id': 'VARCHAR',
        'unit_concept_id': 'VARCHAR',
        'range_low': 'VARCHAR',
        'range_high': 'VARCHAR',
        'provider_id': 'VARCHAR',
        'visit_occurrence_id': 'VARCHAR',
        'visit_detail_id': 'VARCHAR',
        'measurement_source_value': 'VARCHAR',
        'measurement_source_concept_id': 'VARCHAR',
        'unit_source_value': 'VARCHAR',
        'value_source_value': 'VARCHAR'
    })