

    select
        distinct
        visit_occurrence_id::integer as "visit_occurrence_id",
        emerge_id::integer as "person_id",
        0::integer as "visit_concept_id", -- derived 
        date_add(birth_date, INTERVAL (age_at_event) YEAR)::DATE as "visit_start_date", -- visits "will all be on the same date"
        null::timestamp as "visit_start_datetime",
        date_add(birth_date, INTERVAL (age_at_event) YEAR)::DATE as "visit_end_date", -- derived - same date as start date?
        null::timestamp as "visit_end_datetime",
        32817::integer as "visit_type_concept_id", -- derived
        null::integer as "provider_id",
        CAST(200000000 AS INTEGER) + CAST(CONCAT('999', src_care_site) AS INTEGER)::integer as "care_site_id",
        null::text as "visit_source_value",
        null::integer as "visit_source_concept_id",
        null::integer as "admitted_from_concept_id",
        null::text as "admitted_from_source_value",
        null::integer as "discharged_to_concept_id",
        null::text as "discharged_to_source_value",
        null::integer as "preceding_visit_occurrence_id",
        encounter_id::integer as "x_encounter_id"
    from "dbt"."main"."emerge_consort_gira_int_visit_occurrences"