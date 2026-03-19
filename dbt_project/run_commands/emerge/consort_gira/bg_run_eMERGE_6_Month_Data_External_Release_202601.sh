#!/bin/bash
# +
#  - Use `dbt seed --full-refresh` to refresh seeds in the database. On its own, `dbt seed` only ensures
#  the presence of the tables from `dbt_project/seeds/*` in the db.


# dbt clean
# dbt deps || { echo "Error: dbt deps failed. Exiting..."; exit 1; }

# dbt seed #--full-refresh

# +
# Source tables
# dbt run --select emerge_consort_gira_src_emerge_person_ex_release_20260123
# dbt run --select emerge_consort_gira_src_emerge_measurement_ex_release_20260127
# dbt run --select emerge_consort_gira_src_emerge_bmi_ex_release_20260128
# dbt run --select emerge_consort_gira_src_emerge_cpt_ex_release_20260129
# dbt run --select emerge_consort_gira_src_emerge_icd_ex_release_20260129
# dbt run --select CONCEPT
# dbt run --select CONCEPT_RELATIONSHIP
# dbt run --select CONCEPT_ANCESTOR
# dbt run --select CONCEPT_CLASS
# dbt run --select CONCEPT_SYNONYM
# dbt run --select DOMAIN
# dbt run --select RELATIONSHIP
# dbt run --select VOCABULARY
# dbt run --select DRUG_STRENGTH


# +
# dbt run --select emerge_consort_gira_lookup_concepts
# dbt run --select emerge_consort_gira_lookup_standards
# dbt run --select emerge_consort_gira_lookup_exclusion

# +
# Intermediate tables
# dbt run --select emerge_consort_gira_int_person_persons  #Corrects race/ethnicity cols and ensures concept_ids are Standard.

# dbt run --select emerge_consort_gira_int_care_sites
# dbt run --select emerge_consort_gira_int_visit_occurrences

# dbt run --select emerge_consort_gira_int_bmi_measurements

# dbt run --select emerge_consort_gira_int_measurement_measurements
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




# +
# Stb tables
# dbt run --select person
# dbt run --select measurement
# dbt run --select observation
# dbt run --select drug_exposure
# dbt run --select visit_occurrence
# dbt run --select condition_occurrence
# dbt run --select care_site
dbt run --select procedure_occurrence
# dbt run --select device_exposure


# dbt run --select observation_period
# dbt run --select visit_detailcd ~

# dbt run --select death
# dbt run --select note
# dbt run --select note_nlp
# dbt run --select specimen
# dbt run --select fact_relationship
# dbt run --select location
# dbt run --select provider
# dbt run --select payer_plan_period
# dbt run --select cost
# dbt run --select drug_era
# dbt run --select dose_era
# dbt run --select condition_era
# dbt run --select episode
# dbt run --select episode_event
# dbt run --select metadata
# dbt run --select cohort
# dbt run --select cohort_definition


# dbt run --select cdm_source
# # dbt run --select concept
# # dbt run --select vocabulary
# # dbt run --select domain
# # dbt run --select concept_class
# # dbt run --select concept_relationship
# # dbt run --select relationship
# # dbt run --select concept_synonym
# # dbt run --select concept_ancestor
# # dbt run --select source_to_concept_map
# # dbt run --select drug_strength

# -
# dbt run --select emerge_consort_gira_int_cpt_none
# dbt run --select emerge_consort_gira_int_icd_none
# dbt run --select emerge_consort_gira_int_bmi_none
# dbt run --select emerge_consort_gira_int_measurement_none


