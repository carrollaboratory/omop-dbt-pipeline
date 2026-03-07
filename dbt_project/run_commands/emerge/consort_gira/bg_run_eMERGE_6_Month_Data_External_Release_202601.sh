#!/bin/bash
dbt clean
dbt deps || { echo "Error: dbt deps failed. Exiting..."; exit 1; }
#dbt seed #--full-refresh # Full-refresh ensures seed tables exist AND are the most current version

# +
# Source tables
# dbt run --select emerge_consort_gira_src_emerge_person_ex_release_20260123
# dbt run --select emerge_consort_gira_src_emerge_measurement_ex_release_20260127
# dbt run --select emerge_consort_gira_src_emerge_bmi_ex_release_20260128
# dbt run --select emerge_consort_gira_src_emerge_cpt_ex_release_20260129
# dbt run --select emerge_consort_gira_src_emerge_icd_ex_release_20260129
# dbt run --select CONCEPT
# dbt run --select CONCEPT_RELATIONSHIP
# dbt run --select emerge_consort_gira_lookup_concepts
# dbt run --select emerge_consort_gira_lookup_standards


# +
# Intermediate tables
# dbt run --select emerge_consort_gira_int_bmi_measurements

# dbt run --select emerge_consort_gira_int_measurement_measurements # too large don't run
# dbt run --select emerge_consort_gira_int_measurement_observations

# dbt run --select emerge_consort_gira_int_cpt_measurements
# dbt run --select emerge_consort_gira_int_cpt_observations
# dbt run --select emerge_consort_gira_int_cpt_procedures
# dbt run --select emerge_consort_gira_int_cpt_drugs
# dbt run --select emerge_consort_gira_int_cpt_devices

# dbt run --select emerge_consort_gira_int_icd_measurements
# dbt run --select emerge_consort_gira_int_icd_observations
# dbt run --select emerge_consort_gira_int_icd_procedures
# dbt run --select emerge_consort_gira_int_icd_conditions

# dbt run --select emerge_consort_gira_int_person_persons

# dbt run --select emerge_consort_gira_lookup_care_sites


# +
# In Progress

# dbt run --select emerge_consort_gira_lookup_care_sites


# +
# Stb tables
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

# +
# Problem children - These have {table}_concept_ids/codes that don't join to the concept table, therefore, their domain is unknown.
# dbt run --select emerge_consort_gira_int_bmi_none
# dbt run --select emerge_consort_gira_int_measurement_none
# dbt run --select emerge_consort_gira_int_person_none
# dbt run --select emerge_consort_gira_int_icd_none
# dbt run --select emerge_consort_gira_int_cpt_none

