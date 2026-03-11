{{ config(materialized='table') }}


    select
    {{ generate_key('icd', 'consort_gira', 'src_index') }}::integer as "procedure_occurrence_id",
    emerge_id::integer as "person_id",
    s_procedure_concept_id::integer as "procedure_concept_id",
    date_add(vo.birth_date, INTERVAL (vo.age_at_event) YEAR)::date as "procedure_date",
    null::timestamp as "procedure_datetime",
    null::text as "procedure_end_date",
    null::timestamp as "procedure_end_datetime",
    32817::integer as "procedure_type_concept_id", -- derived
    null::integer as "modifier_concept_id",
    null::integer as "quantity",
    null::integer as "provider_id",
    vo.visit_occurrence_id::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    icd_code::text as "procedure_source_value",
    icd_id::text as "procedure_source_concept_id",
    null::text as "modifier_source_value",
    icd.row_id::text as "x_row_id",
    encounter_id::integer as "x_encounter_id",
    gira_ror::text as "x_gira_ror"
    from {{ ref('emerge_consort_gira_int_icd_procedures') }} as icd
    left join {{ ref('emerge_consort_gira_int_visit_occurrences') }} as vo
    using (emerge_id, encounter_id)

    
    union all
    
    select
    {{ generate_key('cpt', 'consort_gira', 'src_index') }}::integer as "procedure_occurrence_id",
    emerge_id::integer as "person_id",
    s_procedure_concept_id::integer as "procedure_concept_id",
    date_add(vo.birth_date, INTERVAL (vo.age_at_event) YEAR)::date as "procedure_date",
    null::timestamp as "procedure_datetime",
    null::text as "procedure_end_date",
    null::timestamp as "procedure_end_datetime",
    32817::integer as "procedure_type_concept_id",
    null::integer as "modifier_concept_id",
    null::integer as "quantity",
    null::integer as "provider_id",
    vo.visit_occurrence_id::integer as "visit_occurrence_id",
    null::integer as "visit_detail_id",
    null::text as "procedure_source_value",
    cpt_id::text as "procedure_source_concept_id",
    null::text as "modifier_source_value",
    cpt.row_id::text as "x_row_id",
    encounter_id::integer as "x_encounter_id",
    gira_ror::text as "x_gira_ror"
    from {{ ref('emerge_consort_gira_int_cpt_procedures') }} as cpt
    left join {{ ref('emerge_consort_gira_int_visit_occurrences') }} as vo
    using (emerge_id, encounter_id)
