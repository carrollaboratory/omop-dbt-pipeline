

    select
    CAST(200000000 AS INTEGER) + CAST(CONCAT('999', site_id) AS INTEGER)::integer as "care_site_id",
    site_name::text as "care_site_name",
    null::integer as "place_of_service_concept_id",
    null::integer as "location_id",
    site_id::text as "care_site_source_value",
    null::text as "place_of_service_source_value"
    from "dbt"."main"."emerge_consort_gira_int_care_sites"