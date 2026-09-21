{{ config(
    alias='procedure_occurrence'
) }}

SELECT
    "procedure_occurrence_id"::INTEGER AS "procedure_occurrence_id",
    "person_id"::INTEGER AS "person_id",
    "procedure_concept_id"::INTEGER AS "procedure_concept_id",
    "procedure_date"::DATE AS "procedure_date",
    "procedure_datetime"::DATETIME AS "procedure_datetime",
    "procedure_type_concept_id"::INTEGER AS "procedure_type_concept_id",
    "modifier_concept_id"::INTEGER AS "modifier_concept_id",
    "quantity"::INTEGER AS "quantity",
    "provider_id"::INTEGER AS "provider_id",
    "visit_occurrence_id"::INTEGER AS "visit_occurrence_id",
    "visit_detail_id"::INTEGER AS "visit_detail_id",
    "procedure_source_value"::TEXT AS "procedure_source_value",
    "procedure_source_concept_id"::INTEGER AS "procedure_source_concept_id",
    "modifier_source_value"::TEXT AS "modifier_source_value"
FROM read_csv('../../_study_data/emerge_legacy/eMERGE_GWAS_OMOP_procedure_occurrence_draft.csv', AUTO_DETECT=FALSE, HEADER=TRUE, nullstr = ["null", "NA", "N/A"], columns={
        'procedure_occurrence_id': 'VARCHAR',
        'person_id': 'VARCHAR',
        'procedure_concept_id': 'VARCHAR',
        'procedure_date': 'VARCHAR',
        'procedure_datetime': 'VARCHAR',
        'procedure_type_concept_id': 'VARCHAR',
        'modifier_concept_id': 'VARCHAR',
        'quantity': 'VARCHAR',
        'provider_id': 'VARCHAR',
        'visit_occurrence_id': 'VARCHAR',
        'visit_detail_id': 'VARCHAR',
        'procedure_source_value': 'VARCHAR',
        'procedure_source_concept_id': 'VARCHAR',
        'modifier_source_value': 'VARCHAR'
    })