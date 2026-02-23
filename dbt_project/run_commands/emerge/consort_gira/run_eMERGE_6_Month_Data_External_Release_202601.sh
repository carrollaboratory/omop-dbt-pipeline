#!/bin/bash
dbt clean
dbt deps || { echo "Error: dbt deps failed. Exiting..."; exit 1; }
dbt seed #--full-refresh

# Source tables
# dbt run --select emerge_consort_gira_src_emerge_person_ex_release_20260123
# dbt run --select emerge_consort_gira_src_emerge_measurement_ex_release_20260127
# dbt run --select emerge_consort_gira_src_emerge_bmi_ex_release_20260128
# dbt run --select emerge_consort_gira_src_emerge_cpt_ex_release_20260129
# dbt run --select emerge_consort_gira_src_emerge_icd_ex_release_20260129

# #  Stb tables
# dbt run --select emerge_consort_gira_stb_person
# dbt run --select emerge_consort_gira_stb_observation_period
# dbt run --select emerge_consort_gira_stb_visit_occurrence
# dbt run --select emerge_consort_gira_stb_visit_detail
# dbt run --select emerge_consort_gira_stb_condition_occurrence
# dbt run --select emerge_consort_gira_stb_drug_exposure
# dbt run --select emerge_consort_gira_stb_procedure_occurrence
# dbt run --select emerge_consort_gira_stb_device_exposure
# dbt run --select emerge_consort_gira_stb_measurement
# dbt run --select emerge_consort_gira_stb_observation
# dbt run --select emerge_consort_gira_stb_death
# dbt run --select emerge_consort_gira_stb_note
# dbt run --select emerge_consort_gira_stb_note_nlp
# dbt run --select emerge_consort_gira_stb_specimen
# dbt run --select emerge_consort_gira_stb_fact_relationship
# dbt run --select emerge_consort_gira_stb_location
# dbt run --select emerge_consort_gira_stb_care_site
# dbt run --select emerge_consort_gira_stb_provider
# dbt run --select emerge_consort_gira_stb_payer_plan_period
# dbt run --select emerge_consort_gira_stb_cost
# dbt run --select emerge_consort_gira_stb_drug_era
# dbt run --select emerge_consort_gira_stb_dose_era
# dbt run --select emerge_consort_gira_stb_condition_era
# dbt run --select emerge_consort_gira_stb_episode
# dbt run --select emerge_consort_gira_stb_episode_event
# dbt run --select emerge_consort_gira_stb_metadata
# dbt run --select emerge_consort_gira_stb_cdm_source
# dbt run --select emerge_consort_gira_stb_concept
# dbt run --select emerge_consort_gira_stb_vocabulary
# dbt run --select emerge_consort_gira_stb_domain
# dbt run --select emerge_consort_gira_stb_concept_class
# dbt run --select emerge_consort_gira_stb_concept_relationship
# dbt run --select emerge_consort_gira_stb_relationship
# dbt run --select emerge_consort_gira_stb_concept_synonym
# dbt run --select emerge_consort_gira_stb_concept_ancestor
# dbt run --select emerge_consort_gira_stb_source_to_concept_map
# dbt run --select emerge_consort_gira_stb_drug_strength
# dbt run --select emerge_consort_gira_stb_cohort
# dbt run --select emerge_consort_gira_stb_cohort_definition

# #  export tables
# dbt run --select omop_54_person
# dbt run --select omop_54_observation_period
# dbt run --select omop_54_visit_occurrence
# dbt run --select omop_54_visit_detail
# dbt run --select omop_54_condition_occurrence
# dbt run --select omop_54_drug_exposure
# dbt run --select omop_54_procedure_occurrence
# dbt run --select omop_54_device_exposure
# dbt run --select omop_54_measurement
# dbt run --select omop_54_observation
# dbt run --select omop_54_death
# dbt run --select omop_54_note
# dbt run --select omop_54_note_nlp
# dbt run --select omop_54_specimen
# dbt run --select omop_54_fact_relationship
# dbt run --select omop_54_location
# dbt run --select omop_54_care_site
# dbt run --select omop_54_provider
# dbt run --select omop_54_payer_plan_period
# dbt run --select omop_54_cost
# dbt run --select omop_54_drug_era
# dbt run --select omop_54_dose_era
# dbt run --select omop_54_condition_era
# dbt run --select omop_54_episode
# dbt run --select omop_54_episode_event
# dbt run --select omop_54_metadata
# dbt run --select omop_54_cdm_source
# dbt run --select omop_54_concept
# dbt run --select omop_54_vocabulary
# dbt run --select omop_54_domain
# dbt run --select omop_54_concept_class
# dbt run --select omop_54_concept_relationship
# dbt run --select omop_54_relationship
# dbt run --select omop_54_concept_synonym
# dbt run --select omop_54_concept_ancestor
# dbt run --select omop_54_source_to_concept_map
# dbt run --select omop_54_drug_strength
# dbt run --select omop_54_cohort
# dbt run --select omop_54_cohort_definition