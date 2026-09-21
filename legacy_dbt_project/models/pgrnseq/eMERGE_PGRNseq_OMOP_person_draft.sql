{{ config(
    alias='person'
) }}

SELECT
    "person_id"::INTEGER AS "person_id",
    "gender_concept_id"::INTEGER AS "gender_concept_id",
    "year_of_birth"::INTEGER AS "year_of_birth",
    "month_of_birth"::INTEGER AS "month_of_birth",
    "day_of_birth"::INTEGER AS "day_of_birth",
    "birth_datetime"::DATETIME AS "birth_datetime",
    "race_concept_id"::INTEGER AS "race_concept_id",
    "ethnicity_concept_id"::INTEGER AS "ethnicity_concept_id",
    "location_id"::INTEGER AS "location_id",
    "provider_id"::INTEGER AS "provider_id",
    "care_site_id"::INTEGER AS "care_site_id",
    "person_source_value"::TEXT AS "person_source_value",
    "gender_source_value"::TEXT AS "gender_source_value",
    "gender_source_concept_id"::INTEGER AS "gender_source_concept_id",
    "race_source_value"::TEXT AS "race_source_value",
    "race_source_concept_id"::INTEGER AS "race_source_concept_id",
    "ethnicity_source_value"::TEXT AS "ethnicity_source_value",
    "ethnicity_source_concept_id"::INTEGER AS "ethnicity_source_concept_id"
FROM read_csv('../../_study_data/emerge_legacy/eMERGE_PGRNseq_OMOP_person_draft.csv', AUTO_DETECT=FALSE, HEADER=TRUE, nullstr = ["null", "NA", "N/A"], columns={
        'person_id': 'VARCHAR',
        'gender_concept_id': 'VARCHAR',
        'year_of_birth': 'VARCHAR',
        'month_of_birth': 'VARCHAR',
        'day_of_birth': 'VARCHAR',
        'birth_datetime': 'VARCHAR',
        'race_concept_id': 'VARCHAR',
        'ethnicity_concept_id': 'VARCHAR',
        'location_id': 'VARCHAR',
        'provider_id': 'VARCHAR',
        'care_site_id': 'VARCHAR',
        'person_source_value': 'VARCHAR',
        'gender_source_value': 'VARCHAR',
        'gender_source_concept_id': 'VARCHAR',
        'race_source_value': 'VARCHAR',
        'race_source_concept_id': 'VARCHAR',
        'ethnicity_source_value': 'VARCHAR',
        'ethnicity_source_concept_id': 'VARCHAR'
    })