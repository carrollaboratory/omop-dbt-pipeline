{{ config(
    alias='specimen'
) }}

SELECT
    "specimen_id"::INTEGER AS "specimen_id",
    "person_id"::INTEGER AS "person_id",
    "specimen_concept_id"::INTEGER AS "specimen_concept_id",
    "specimen_type_concept_id"::INTEGER AS "specimen_type_concept_id",
    "specimen_date"::DATE AS "specimen_date",
    "specimen_datetime"::DATETIME AS "specimen_datetime",
    "quantity"::FLOAT AS "quantity",
    "unit_concept_id"::INTEGER AS "unit_concept_id",
    "anatomic_site_concept_id"::INTEGER AS "anatomic_site_concept_id",
    "disease_status_concept_id"::INTEGER AS "disease_status_concept_id",
    "specimen_source_id"::TEXT AS "specimen_source_id",
    "specimen_source_value"::TEXT AS "specimen_source_value",
    "unit_source_value"::TEXT AS "unit_source_value",
    "anatomic_site_source_value"::TEXT AS "anatomic_site_source_value",
    "disease_status_source_value"::TEXT AS "disease_status_source_value"
FROM read_csv('../../_study_data/emerge_legacy/eMERGE_PGRNseq_OMOP_specimen_draft.csv', AUTO_DETECT=FALSE, HEADER=TRUE, nullstr = ["null", "NA", "N/A"], columns={
        'specimen_id': 'VARCHAR',
        'person_id': 'VARCHAR',
        'specimen_concept_id': 'VARCHAR',
        'specimen_type_concept_id': 'VARCHAR',
        'specimen_date': 'VARCHAR',
        'specimen_datetime': 'VARCHAR',
        'quantity': 'VARCHAR',
        'unit_concept_id': 'VARCHAR',
        'anatomic_site_concept_id': 'VARCHAR',
        'disease_status_concept_id': 'VARCHAR',
        'specimen_source_id': 'VARCHAR',
        'specimen_source_value': 'VARCHAR',
        'unit_source_value': 'VARCHAR',
        'anatomic_site_source_value': 'VARCHAR',
        'disease_status_source_value': 'VARCHAR'
    })