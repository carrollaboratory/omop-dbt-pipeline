{{ config(
    alias='concept'
) }}

SELECT
    "concept_id"::INTEGER AS "concept_id",
    "concept_name"::TEXT AS "concept_name",
    "domain_id"::TEXT AS "domain_id",
    "vocabulary_id"::TEXT AS "vocabulary_id",
    "concept_class_id"::TEXT AS "concept_class_id",
    "standard_concept"::TEXT AS "standard_concept",
    "concept_code"::TEXT AS "concept_code",
    "valid_start_date"::DATE AS "valid_start_date",
    "valid_end_date"::DATE AS "valid_end_date",
    "invalid_reason"::TEXT AS "invalid_reason"
FROM read_csv('../../_study_data/emerge_legacy/eMERGE_PGRNseq_OMOP_concept_draft.csv', AUTO_DETECT=FALSE, HEADER=TRUE, nullstr = ["null", "NA", "N/A"], columns={
        'concept_id': 'VARCHAR',
        'concept_name': 'VARCHAR',
        'domain_id': 'VARCHAR',
        'vocabulary_id': 'VARCHAR',
        'concept_class_id': 'VARCHAR',
        'standard_concept': 'VARCHAR',
        'concept_code': 'VARCHAR',
        'valid_start_date': 'VARCHAR',
        'valid_end_date': 'VARCHAR',
        'invalid_reason': 'VARCHAR'
    })