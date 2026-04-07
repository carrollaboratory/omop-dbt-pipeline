
with base as (
    select distinct emerge_id
    from "dbt"."main"."emerge_consort_gira_src_emerge_measurement_ex_release_20260127"
    union
    select distinct emerge_id
    from "dbt"."main"."emerge_consort_gira_src_emerge_bmi_ex_release_20260128"
    union
    select distinct emerge_id
    from "dbt"."main"."emerge_consort_gira_src_emerge_cpt_ex_release_20260129"
    union
    select distinct emerge_id
    from "dbt"."main"."emerge_consort_gira_src_emerge_icd_ex_release_20260129"
)
select emerge_id from base
where emerge_id not in (select distinct emerge_id from "dbt"."main"."emerge_consort_gira_src_emerge_person_ex_release_20260123")