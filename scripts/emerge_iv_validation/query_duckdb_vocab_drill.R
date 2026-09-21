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
library(dplyr)
library(glue)

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


concept_map <- list(
  measurement = c(
    measurement_concept_id = "standard",
    measurement_source_concept_id = "source",
    measurement_type_concept_id = "type",
    operator_concept_id = "operator",
    value_as_concept_id = "value",
    unit_concept_id = "unit"
  ),
  
  observation = c(
    observation_concept_id = "standard",
    observation_source_concept_id = "source",
    observation_type_concept_id = "type",
    value_as_concept_id = "value",
    qualifier_concept_id = "qualifier",
    unit_concept_id = "unit"
  ),
  
  condition_occurrence = c(
    condition_concept_id = "standard",
    condition_source_concept_id = "source",
    condition_type_concept_id = "type",
    condition_status_concept_id = "status"
  ),
  
  device_exposure = c(
    device_concept_id = "standard",
    device_source_concept_id = "source",
    device_type_concept_id = "type"
  ),
  
  drug_exposure = c(
    drug_concept_id = "standard",
    drug_source_concept_id = "source",
    drug_type_concept_id = "type",
    route_concept_id = "route"
  ),
  
  procedure_occurrence = c(
    procedure_concept_id = "standard",
    procedure_source_concept_id = "source",
    procedure_type_concept_id = "type",
    modifier_concept_id = "modifier"
  ),
  
  visit_occurrence = c(
    visit_concept_id = "standard",
    visit_source_concept_id = "source",
    visit_type_concept_id = "type"
  )
)

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


print("=== VOCABULARY - None vs '' result ===")
investigate_vocab_nones <- execute(
  "SELECT 
   distinct(vocabulary_id),
   count(*) as n
   FROM dev_202609_6mo_lookups.emerge_consort_gira_lookup_concepts  c
   WHERE vocabulary_id NOT IN ('Race', 'LOINC', 'ICD10', 'UCUM', 'SNOMED', 'ICD10PCS', 'Gender', 'ICD10CN', 'ICD9CM', 'ICD9Proc', 'ICD9ProcCN', 'Ethnicity', 'CPT4', 'ICD10CM')
   OR vocabulary_id IS NULL
   GROUP BY vocabulary_id;
")
print(investigate_vocab_nones)

print("=== VOCABULARY - None vs '' result ===")
investigate_vocab_nones2 <- execute(
  "SELECT 
   *
   FROM dev_202609_6mo_lookups.emerge_consort_gira_lookup_concepts  c
   WHERE 
   --vocabulary_id NOT IN ('Race', 'LOINC', 'ICD10', 'UCUM', 'SNOMED', 'ICD10PCS', 'Gender', 'ICD10CN', 'ICD9CM', 'ICD9Proc', 'ICD9ProcCN', 'Ethnicity', 'CPT4', 'ICD10CM')
   --OR 
   vocabulary_id IS NULL
")
print(investigate_vocab_nones2)





print("=== VOCABULARY in harmonized ===")

queries <- unlist(lapply(names(concept_map), function(table) {
  
  columns <- concept_map[[table]]
  
  mapply(
    function(column, concept_type) {
      glue(
        "SELECT '{table}' AS table_name,
                {column} AS concept_id,
                '{concept_type}' AS concept_type
         FROM dev_202609_omop.{table}"
      )
    },
    names(columns),
    unname(columns)
  )
}))


sql <- paste(queries, collapse = "\nUNION ALL\n")
concepts <- dbGetQuery(con, sql)
sql <- paste0(
  "SELECT DISTINCT 
      x.table_name,
      x.concept_id,
      x.concept_type,
      c.vocabulary_id,
      c.concept_name
   FROM (",
  paste(queries, collapse = "\nUNION ALL\n"),
  ") x
   LEFT JOIN dev_202609_vocab.concept c
     USING (concept_id)
   WHERE x.concept_id IS NOT NULL
     AND x.concept_id <> 0
   ORDER BY x.table_name, x.concept_id, x.concept_type"
)

concepts <- dbGetQuery(con, sql)
print(concepts)






sql2 <- paste(queries, collapse = "\nUNION ALL\n")
concepts2 <- dbGetQuery(con, sql)
sql2 <- paste0(
  "SELECT DISTINCT 
      x.table_name,
      x.concept_type,
      c.vocabulary_id as s_vocabulary_id,
      count(x.concept_id) as n
   FROM (",
  paste(queries, collapse = "\nUNION ALL\n"),
  ") x
   LEFT JOIN dev_202609_vocab.concept c
     USING (concept_id)
   WHERE x.concept_id IS NOT NULL
     AND x.concept_id <> 0
   GROUP BY x.table_name, c.vocabulary_id, x.concept_type
   ORDER BY table_name,vocabulary_id,concept_type
  "
)

concepts2 <- dbGetQuery(con, sql2)
print(concepts2)




sql3 <- paste(queries, collapse = "\nUNION ALL\n")
concepts3 <- dbGetQuery(con, sql3)
sql3 <- paste0(
  "SELECT DISTINCT 
      x.table_name,
      c.vocabulary_id as vocabulary_ids,
      count(*)
   FROM (",
  paste(queries, collapse = "\nUNION ALL\n"),
  ") x
   LEFT JOIN dev_202609_vocab.concept c
     USING (concept_id)
   WHERE x.concept_id IS NOT NULL
     AND x.concept_type = 'standard'
     GROUP BY c.vocabulary_id, table_name
     ORDER BY table_name
  "
)

concepts3 <- dbGetQuery(con, sql3)
print(concepts3)