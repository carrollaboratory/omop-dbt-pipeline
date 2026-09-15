# ============================================================================
# Tool to query tables in the duckdb database.
# Create new cells for study specific analysis/validation.
# ============================================================================

# 1. Setup and Dependencies ------------------------------------------------

if(!require("duckdb")) install.packages("duckdb")
if(!require("DBI")) install.packages("DBI")
if(!require("tidyverse")) install.packages("tidyverse")
if(!require("openxlsx")) install.packages("openxlsx")

library(duckdb)
library(DBI)
library(tidyverse)
library(openxlsx)

# 2. Environment Setup -------------------------------------------------------

bucket <- Sys.getenv("WORKSPACE_BUCKET")
if (bucket == "") {
  bucket <- "bucket_placeholder"
}

# 3. Connect to DuckDB -------------------------------------------------------

drv <- duckdb(dbdir = "~/dbt.duckdb_eiv_6mo")
con <- dbConnect(drv)

# 4. Helper Function to Execute Queries ------------------------------------

execute <- function(query) {
  result <- dbGetQuery(con, query)
  return(result)
}

# 5. List Tables and Get Dimensions ------------------------------------------

table_names <- execute(
  "SELECT table_name FROM information_schema.tables 
   WHERE table_schema like 'dev_202609_%'
   AND (table_name like '%src%' OR table_name like '%int%')"
)

print(table_names)



table_names <- execute(
  "SELECT table_name FROM information_schema.tables 
   WHERE table_schema like 'dev_202609_omop%'
   "
)

shapes <- list()
for (t in table_names$table_name) {
  nrows <- execute(sprintf('SELECT COUNT(*) AS nrows FROM "dev_202609_omop"."%s"', t))$nrows[1]
  
  ncols <- execute(sprintf(
    "SELECT COUNT(*) AS ncols
     FROM information_schema.columns
     WHERE table_schema = 'dev_202609_omop'
     AND table_name = '%s'", t
  ))$ncols[1]
  
  shapes[[length(shapes) + 1]] <- data.frame(
    table_name = t,
    nrows = nrows,
    ncols = ncols
  )
}

shape_df <- do.call(rbind, shapes) %>% arrange(table_name)
print(shape_df)

# ============================================================================
# ANALYSIS SECTION
# ============================================================================

# 6. Withdrawal Status Analysis -----------------------------------------------

print("=== WITHDRAWAL STATUS ===")
withdrawal_status <- execute(
  "SELECT 
   sum(case when withdrawal_status = 1 then 1 else 0 end) as active,
   sum(case when withdrawal_status = 0 then 1 else 0 end) as withdrawn,
   sum(case when (withdrawal_status not in (1, 0) or withdrawal_status is null) then 1 else 0 end) as unexpected_status
   FROM dev_202609_6mo_int.emerge_consort_gira_int_person_persons"
)
print(withdrawal_status)

# ============================================================================
# MEASUREMENT SECTION
# ============================================================================

# 7. Measurement Concept Analysis -------------------------------------------

print("=== MEASUREMENT CONCEPT ANALYSIS ===")
measurement_concepts <- execute(
  "WITH concept_meas as (
   SELECT *
   FROM dev_202609_6mo_src.emerge_consort_gira_src_emerge_measurement_ex_release_20260127 meas_src
   LEFT JOIN (SELECT
              concept_id as mci_concept_id,
              concept_code as mci_concept_code,
              standard_concept as mci_standard_concept,
              vocabulary_id as mci_vocabulary_id,
              domain_id as mci_domain_id,
              concept_class_id as mci_concept_class_id
              FROM dev_202609_6mo_lookups.emerge_consort_gira_lookup_concepts  
              ) AS mci
       ON meas_src.measurement_concept_id = mci.mci_concept_id
   ),
   agg_meas AS (
   SELECT 
       measurement_concept_id,
       CASE 
           WHEN mci_concept_id IS NOT NULL THEN 1 
           ELSE 0 
       END AS has_join,
       mci_standard_concept,
       mci_vocabulary_id,
       mci_domain_id,
       mci_concept_class_id
   FROM concept_meas
   )
   SELECT 
       SUM(has_join) AS rows_with_concept_match,
       SUM(CASE WHEN has_join = 0 THEN 1 ELSE 0 END) AS rows_without_concept_match,
       SUM(CASE WHEN mci_standard_concept = 'S' THEN 1 ELSE 0 END) AS rows_with_standard_concept,
       STRING_AGG(DISTINCT mci_vocabulary_id, ', ') AS vocabulary_ids,
       STRING_AGG(DISTINCT mci_domain_id, ', ') AS domain_ids,
       STRING_AGG(DISTINCT mci_concept_class_id, ', ') AS concept_class_ids
   FROM agg_meas
   group by mci_domain_id"
)
print(measurement_concepts)

# 8. Measurement Range_Low Validation ----------------------------------------

print("=== MEASUREMENT RANGE_LOW VALIDATION ===")
range_low_issues <- execute(
  "SELECT
   range_low,
   COUNT(*) AS row_count
   FROM dev_202609_6mo_src.emerge_consort_gira_src_emerge_measurement_ex_release_20260127
   WHERE range_low IS NOT NULL
   AND TRY_CAST(range_low AS INTEGER) IS NULL
   GROUP BY range_low
   ORDER BY row_count DESC, range_low"
)
print(range_low_issues)
# TODO: range_low should be castable to float --> Measurement table

# 9. Measurement Range_High Validation ----------------------------------------

print("=== MEASUREMENT RANGE_HIGH VALIDATION ===")
range_high_issues <- execute(
  "SELECT
   range_high,
   COUNT(*) AS row_count
   FROM dev_202609_6mo_src.emerge_consort_gira_src_emerge_measurement_ex_release_20260127
   WHERE range_high IS NOT NULL
   AND TRY_CAST(range_high AS INTEGER) IS NULL
   GROUP BY range_high
   ORDER BY row_count DESC, range_high"
)
print(range_high_issues)
# TODO: range_high should be castable to float --> Measurement table

# ============================================================================
# PERSON SECTION
# ============================================================================

# 10. Person Year of Birth Analysis -------------------------------------------

print("=== PERSON YEAR OF BIRTH ===")
person_yob <- execute(
  "SELECT 
   SUM(CASE WHEN year_of_birth is not null then 1 else 0 end) as yob_exists,
   SUM(CASE WHEN year_of_birth is null then 1 else 0 end) as yob_null
   FROM dev_202609_6mo_src.emerge_consort_gira_src_emerge_person_ex_release_20260123"
)
print(person_yob)
# TODO: yob is used for measurement.measurement_date. What to do when NULL?

# ============================================================================
# BMI SECTION
# ============================================================================

# 11. BMI Concept Analysis ---------------------------------------------------

print("=== BMI CONCEPT ANALYSIS ===")
bmi_concepts <- execute(
  "WITH concept_meas as (
   SELECT *
   FROM dev_202609_6mo_src.emerge_consort_gira_src_emerge_bmi_ex_release_20260128 meas_src
   LEFT JOIN (SELECT
              concept_id as mci_concept_id,
              concept_code as mci_concept_code,
              standard_concept as mci_standard_concept,
              vocabulary_id as mci_vocabulary_id,
              domain_id as mci_domain_id,
              concept_class_id as mci_concept_class_id
              FROM dev_202609_6mo_lookups.emerge_consort_gira_lookup_concepts  
              ) AS mci
       ON meas_src.measurement_concept_id = mci.mci_concept_id
   ),
   agg_meas AS (
   SELECT 
       measurement_concept_id,
       CASE 
           WHEN mci_concept_id IS NOT NULL THEN 1 
           ELSE 0 
       END AS has_join,
       mci_standard_concept,
       mci_vocabulary_id,
       mci_domain_id,
       mci_concept_class_id
   FROM concept_meas
   )
   SELECT 
       SUM(has_join) AS rows_with_concept_match,
       SUM(CASE WHEN has_join = 0 THEN 1 ELSE 0 END) AS rows_without_concept_match,
       SUM(CASE WHEN mci_standard_concept = 'S' THEN 1 ELSE 0 END) AS rows_with_standard_concept,
       STRING_AGG(DISTINCT mci_vocabulary_id, ', ') AS vocabulary_ids,
       STRING_AGG(DISTINCT mci_domain_id, ', ') AS domain_ids,
       STRING_AGG(DISTINCT mci_concept_class_id, ', ') AS concept_class_ids
   FROM agg_meas
   group by mci_domain_id"
)
print(bmi_concepts)

# ============================================================================
# CPT SECTION
# ============================================================================

# 12. CPT Concept Analysis ---------------------------------------------------

print("=== CPT CONCEPT ANALYSIS ===")
cpt_concepts <- execute(
  "WITH concept_meas as (
   SELECT *
   FROM dev_202609_6mo_src.emerge_consort_gira_src_emerge_cpt_ex_release_20260129 meas_src
   LEFT JOIN (SELECT
              concept_id as mci_concept_id,
              concept_code as mci_concept_code,
              standard_concept as mci_standard_concept,
              vocabulary_id as mci_vocabulary_id,
              domain_id as mci_domain_id,
              concept_class_id as mci_concept_class_id
              FROM dev_202609_6mo_lookups.emerge_consort_gira_lookup_concepts  
              ) AS mci
       ON meas_src.cpt_code = mci.mci_concept_code
   ),
   agg_meas AS (
   SELECT 
       cpt_code,
       CASE 
           WHEN mci_concept_code IS NOT NULL THEN 1 
           ELSE 0 
       END AS has_join,
       mci_standard_concept,
       mci_vocabulary_id,
       mci_domain_id,
       mci_concept_class_id
   FROM concept_meas
   )
   SELECT 
       SUM(has_join) AS rows_with_concept_match,
       SUM(CASE WHEN has_join = 0 THEN 1 ELSE 0 END) AS rows_without_concept_match,
       SUM(CASE WHEN mci_standard_concept = 'S' THEN 1 ELSE 0 END) AS rows_with_standard_concept,
       STRING_AGG(DISTINCT mci_vocabulary_id, ', ') AS vocabulary_ids,
       STRING_AGG(DISTINCT mci_domain_id, ', ') AS domain_ids,
       STRING_AGG(DISTINCT mci_concept_class_id, ', ') AS concept_class_ids
   FROM agg_meas
   group by mci_domain_id"
)
print(cpt_concepts)

# ============================================================================
# ICD SECTION
# ============================================================================

# 13. ICD Concept Analysis ---------------------------------------------------

print("=== ICD CONCEPT ANALYSIS ===")
icd_concepts <- execute(
  "WITH concept_icd as (
   SELECT *
   FROM dev_202609_6mo_src.emerge_consort_gira_src_emerge_icd_ex_release_20260129 icd_src
   LEFT JOIN (SELECT
              concept_id as ic_concept_id,
              concept_code as ic_concept_code,
              standard_concept as ic_standard_concept,
              vocabulary_id as ic_vocabulary_id,
              domain_id as ic_domain_id,
              concept_class_id as ic_concept_class_id
              FROM dev_202609_6mo_lookups.emerge_consort_gira_lookup_concepts  
              ) AS ic
       ON icd_src.icd_code = ic.ic_concept_code
   ),
   agg_icd AS (
   SELECT 
       icd_code,
       CASE 
           WHEN icd_code IS NOT NULL THEN 1 
           ELSE 0 
       END AS has_join,
       ic_concept_id,
       ic_standard_concept,
       ic_vocabulary_id,
       ic_domain_id,
       ic_concept_class_id
   FROM concept_icd
   )
   SELECT 
       ic_domain_id,
       SUM(has_join) AS rows_with_concept_match,
       SUM(CASE WHEN has_join = 0 THEN 1 ELSE 0 END) AS rows_without_concept_match,
       SUM(CASE WHEN ic_standard_concept = 'S' THEN 1 ELSE 0 END) AS rows_with_standard_concept,
       STRING_AGG(DISTINCT ic_vocabulary_id, ', ') AS vocabulary_ids,
       STRING_AGG(DISTINCT ic_domain_id, ', ') AS domain_ids,
       STRING_AGG(DISTINCT ic_concept_class_id, ', ') AS concept_class_ids
   FROM agg_icd
   GROUP BY ic_domain_id"
)
print(icd_concepts)

# ============================================================================
# VOCABULARY SECTION
# ============================================================================

# 14. Vocabulary Analysis ----------------------------------------------------

print("=== VOCABULARY ANALYSIS ===")
vocabularies <- execute(
  "SELECT 
   distinct vocabulary_id
   FROM dev_202609_6mo_lookups.emerge_consort_gira_lookup_concepts  c"
)
print(vocabularies)

# TODO: Add tests to a 'src_data/concept_info' model (int?) to assert domains 
# are expected as well as vocabularies.
# EX: If the src measurement table is refreshed and now has a few procedures, 
# or rows that don't join to the vocab at all.



# Check for withdrawn persons in all OMOP tables
withdrawn_check <- list()

for (t in table_names$table_name) {
  tryCatch({
    # First check if person_id column exists in this table
    has_person_id <- execute(sprintf(
      "SELECT COUNT(*) AS col_count
       FROM information_schema.columns
       WHERE table_schema = 'dev_202609_omop'
       AND table_name = '%s'
       AND column_name = 'person_id'", t
    ))$col_count[1]
    
    # Only query if person_id exists
    if (has_person_id > 0) {
      result <- execute(sprintf(
        "SELECT
         '%s' as table_name,
         COUNT(DISTINCT CASE 
           WHEN CAST(person_id AS STRING) NOT IN 
             (SELECT emerge_id FROM dev_202609_6mo_src.emerge_consort_gira_src_emerge_6_month_consent_ds_20260731) 
           THEN person_id 
         END) as n_withdrawn,
         COUNT(DISTINCT person_id) as n_total
         FROM dev_202609_omop.%s", t, t
      ))
      
      withdrawn_check[[length(withdrawn_check) + 1]] <- result
      print(paste("✓", t, "- Total:", result$n_total[1], "| Withdrawn:", result$n_withdrawn[1]))
      
    } else {
      print(paste("⊘", t, "- No person_id column"))
    }
    
  }, error = function(e) {
    print(paste("✗ Error processing", t, ":", e$message))
  })
}

# Combine all results
withdrawn_results <- do.call(rbind, withdrawn_check)
rownames(withdrawn_results) <- NULL

print(withdrawn_results)



caresites <- execute(
  "SELECT distinct care_site_name
  FROM dev_202609_omop.care_site
  order by 1
   "
)
print(caresites)



# Find all tables and columns with x_ prefix in dev_202609_omop schema
x_prefix_columns <- execute(
  "SELECT
   table_name,
   column_name
   FROM information_schema.columns
   WHERE table_schema = 'dev_202609_omop'
   AND column_name LIKE 'x_%'
   ORDER BY table_name, column_name"
)

print(x_prefix_columns)



# Get summary of tables with x_ columns
x_prefix_summary <- execute(
  "SELECT
   table_name,
   COUNT(*) as n_x_columns,
   STRING_AGG(column_name, ', ') as x_columns
   FROM information_schema.columns
   WHERE table_schema = 'dev_202609_omop'
   AND column_name LIKE 'x_%'
   GROUP BY table_name
   ORDER BY n_x_columns DESC, table_name"
)

print(x_prefix_summary)

withdrawal_by_site <- execute(
  "SELECT
   s.site_name as site_name,
   sum(case when withdrawal_status = 1 then 1 else 0 end) as active_in_src_person,
   sum(case when withdrawal_status = 0 then 1 else 0 end) as withdrawn_in_src_person,
   sum(case when active_in_new_src_consent = 1 then 1 else 0 end) as active_in_new_src_consent,
    sum(case when active_in_harmonized_person = 1 then 1 else 0 end) as active_in_harmonized_person,
   count(*) as n_records
   FROM dev_202609_6mo_src.emerge_consort_gira_src_emerge_person_ex_release_20260123 p
   LEFT JOIN dev_202609_6mo_int.emerge_consort_gira_int_care_sites s
     ON substring(p.emerge_id, 1, 2) = s.site_id
   LEFT JOIN (SELECT emerge_id, 1 as active_in_new_src_consent FROM dev_202609_6mo_src.emerge_consort_gira_src_emerge_6_month_consent_ds_20260731) ex
     ON p.emerge_id = ex.emerge_id
   LEFT JOIN (SELECT person_id, 1 as active_in_harmonized_person FROM dev_202609_omop.person) hp
     ON p.emerge_id = hp.person_id
   GROUP BY s.site_name, p.withdrawal_status, active_in_new_src_consent
   ORDER BY site_name, withdrawal_status"
)

print(withdrawal_by_site)


# Create a workbook with multiple sheets
wb <- createWorkbook()


# Add each result as a separate sheet
addWorksheet(wb, "Withdrawal by site")
writeData(wb, "Withdrawal by site", withdrawal_by_site)

addWorksheet(wb, "x_ cols summary")
writeData(wb, "x_ cols summary", x_prefix_summary)

addWorksheet(wb, "Range Low Issues")
writeData(wb, "Range Low Issues", range_low_issues)

addWorksheet(wb, "Range High Issues")
writeData(wb, "Range High Issues", range_high_issues)

addWorksheet(wb, "Measurement Concepts")
writeData(wb, "Measurement Concepts", measurement_concepts)

addWorksheet(wb, "BMI Concepts")
writeData(wb, "BMI Concepts", bmi_concepts)

addWorksheet(wb, "CPT Concepts")
writeData(wb, "CPT Concepts", cpt_concepts)

addWorksheet(wb, "ICD Concepts")
writeData(wb, "ICD Concepts", icd_concepts)

addWorksheet(wb, "Vocabularies")
writeData(wb, "Vocabularies", vocabularies)

addWorksheet(wb, "Table Dimensions")
writeData(wb, "Table Dimensions", shape_df)

# Save the workbook
saveWorkbook(wb, "~/pipelines/_study_data/consort_gira/validation/analysis_results.xlsx", overwrite = TRUE)
print("Results exported to results/analysis_results.xlsx")


# dbDisconnect(con)
