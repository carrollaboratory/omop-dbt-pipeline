{{ config(
    alias='condition_occurrence'
) }}

SELECT
    "condition_occurrence_id"::INTEGER AS "condition_occurrence_id",
    "person_id"::INTEGER AS "person_id",
    "condition_concept_id"::INTEGER AS "condition_concept_id",
    "condition_start_date"::DATE AS "condition_start_date",
    "condition_start_datetime"::DATETIME AS "condition_start_datetime",
    "condition_end_date"::DATE AS "condition_end_date",
    "condition_end_datetime"::DATETIME AS "condition_end_datetime",
    "condition_type_concept_id"::INTEGER AS "condition_type_concept_id",
    "condition_status_concept_id"::INTEGER AS "condition_status_concept_id",
    "stop_reason"::TEXT AS "stop_reason",
    "provider_id"::INTEGER AS "provider_id",
    "visit_occurrence_id"::INTEGER AS "visit_occurrence_id",
    "visit_detail_id"::INTEGER AS "visit_detail_id",
    "condition_source_value"::TEXT AS "condition_source_value",
    "condition_source_concept_id"::INTEGER AS "condition_source_concept_id",
    "condition_status_source_value"::TEXT AS "condition_status_source_value"
FROM read_csv('../../_study_data/emerge_legacy/eMERGEseq_OMOP_condition_occurrence_draft.csv', AUTO_DETECT=FALSE, HEADER=TRUE, nullstr = ["null", "NA", "N/A"], columns={
        'condition_occurrence_id': 'VARCHAR',
        'person_id': 'VARCHAR',
        'condition_concept_id': 'VARCHAR',
        'condition_start_date': 'VARCHAR',
        'condition_start_datetime': 'VARCHAR',
        'condition_end_date': 'VARCHAR',
        'condition_end_datetime': 'VARCHAR',
        'condition_type_concept_id': 'VARCHAR',
        'condition_status_concept_id': 'VARCHAR',
        'stop_reason': 'VARCHAR',
        'provider_id': 'VARCHAR',
        'visit_occurrence_id': 'VARCHAR',
        'visit_detail_id': 'VARCHAR',
        'condition_source_value': 'VARCHAR',
        'condition_source_concept_id': 'VARCHAR',
        'condition_status_source_value': 'VARCHAR'
    })