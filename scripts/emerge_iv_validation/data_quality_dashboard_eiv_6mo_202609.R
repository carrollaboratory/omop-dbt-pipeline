

## Data Quality Dashboard 
### For more about the dqd and this script: https://data.ohdsi.org/DataQualityDashboard/


# DQD 1 - R imports
# ---------------------------------------------------------------
if(!require("duckdb")) {install.packages("duckdb")}
if(!require("DataQualityDashboard")) {remotes::install_github("OHDSI/DataQualityDashboard")}
if(!require("Achilles")) {remotes::install_github("OHDSI/Achilles")}
library(tidyverse)


library(DatabaseConnector)
library(Achilles)
library(tidyverse) 
library(DatabaseConnector)

# DQD 2 - Connect to local db -----------------------------------------------------
connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms="duckdb",
    server = "~/dbt.duckdb_eiv_6mo"
  )
con <- connect(connectionDetails)

# dbDisconnect(con)


# DQD 3 - Configurations to run the dqd -------------------------------------------
cdmDatabaseSchema <- "dev_202609_omop" # the fully qualified database schema name of the CDM
resultsDatabaseSchema <- "main" # the fully qualified database schema name of the results schema (that you can write to)
vocabDatabaseSchema <- "dev_202609_vocab"
cdmSourceName <- "omop5.4" # a human readable name for your CDM source. Used for the output data.
cdmVersion <- "5.4" # the CDM version you are targeting. Currently supports 5.2, 5.3, and 5.3

numThreads <- 1 
sqlOnly <- FALSE # set to TRUE if you just want to get the SQL scripts and not actually run the queries

outputFolder <- "~/pipelines/_study_data/consort_gira/validation"
outputFile <- "results.csv"

verboseMode <- TRUE # set to FALSE if you don't want the logs to be printed to the console
writeToTable <- TRUE # set to FALSE if you want to skip writing to a SQL table in the results schema
writeTableName <- "dqdashboard_results"

checkLevels <- c("TABLE", "FIELD", "CONCEPT")

tablesToExclude <- c('OBSERVATION_PERIOD',
                     'VISIT_DETAIL',
                     'DEATH',
                     'NOTE',
                     'NOTE_NLP',
                     'SPECIMEN',
                     'FACT_RELATIONSHIP',
                     'LOCATION',
                     'PROVIDER',
                     'PAYER_PLAN_PERIOD',
                     'COST',
                     'DRUG_ERA',
                     'DOSE_ERA',
                     'CONDITION_ERA',
                     'EPISODE',
                     'EPISODE_EVENT',
                     'METADATA',
                     'CONCEPT',
                     'VOCABULARY',
                     'DOMAIN',
                     'CONCEPT_CLASS',
                     'CONCEPT_RELATIONSHIP',
                     'RELATIONSHIP',
                     'CONCEPT_SYNONYM',
                     'CONCEPT_ANCESTOR',
                     'SOURCE_TO_CONCEPT_MAP',
                     'DRUG_STRENGTH',
                     'COHORT',
                     'COHORT_DEFINITION',
                     'CDM_SOURCE'
                     )

# DQD 4 - Run the DQD using the variables set above. ------------------------------

DataQualityDashboard::executeDqChecks(connectionDetails = connectionDetails,
                                      cdmDatabaseSchema = cdmDatabaseSchema,
                                      resultsDatabaseSchema = resultsDatabaseSchema,
                                      vocabDatabaseSchema = vocabDatabaseSchema,
                                      cdmSourceName = cdmSourceName,
                                      cdmVersion = cdmVersion,
                                      numThreads = numThreads,
                                      sqlOnly = sqlOnly,
                                      outputFolder = outputFolder,
                                      outputFile = outputFile,
                                      verboseMode = verboseMode,
                                      writeToTable = writeToTable,
                                      checkLevels = checkLevels,
                                      tablesToExclude = tablesToExclude)



# View Results - Open the shiny output ---------------------------------------------------
ParallelLogger::launchLogViewer(logFileName = file.path(outputFolder, cdmSourceName,
                                                        sprintf("log_DqDashboard_%s.txt", cdmSourceName)))


# Push files to the workspace bucket --------------------------------------
system2("gcloud", c("storage","cp","-r","~/legacy_emerge/output_20260825_emergeseq/", "$WORKSPACE_BUCKET/legacy_emerge_validation/emergeseq"))



# Push this R script to the bucket --------------------------------------
anal_path <- path.expand("~/legacy_emerge/dqd_legacy_emerge.R")
system2("gcloud", c("storage","cp","-r", anal_path, "$WORKSPACE_BUCKET/legacy_emerge_validation/"))


# Query the results table from DuckDB
dqd_table_results <- execute(
  "SELECT * FROM results_schema.dqdashboard_results"
)

# Export to CSV
write.csv(dqd_table_results, 
          "results/dqd_results.csv",
          row.names = FALSE)

print("✓ Results exported from SQL table")

