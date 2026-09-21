{{ config(
    alias='care_site'
) }}

SELECT
    "care_site_id"::INTEGER AS "care_site_id",
    "care_site_name"::TEXT AS "care_site_name",
    "place_of_service_concept_id"::INTEGER AS "place_of_service_concept_id",
    "location_id"::INTEGER AS "location_id",
    "care_site_source_value"::TEXT AS "care_site_source_value",
    "place_of_service_source_value"::TEXT AS "place_of_service_source_value"
FROM read_csv('../../_study_data/emerge_legacy/eMERGE_PGRNseq_OMOP_care_site_draft.csv', AUTO_DETECT=FALSE, HEADER=TRUE, nullstr = ["null", "NA", "N/A"], columns={
        'care_site_id': 'VARCHAR',
        'care_site_name': 'VARCHAR',
        'place_of_service_concept_id': 'VARCHAR',
        'location_id': 'VARCHAR',
        'care_site_source_value': 'VARCHAR',
        'place_of_service_source_value': 'VARCHAR'
    })

-- SELECT * FROM 'care_site.csv';
