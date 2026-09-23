# ============================================================================
#Created to ensure the decimals in the src data are not being dropped within the dbt pipeline. 
#Otherwise - handy code.
# ============================================================================


if(!require("duckdb")) install.packages("duckdb")
if(!require("DBI")) install.packages("DBI")
if(!require("tidyverse")) install.packages("tidyverse")
if(!require("openxlsx")) install.packages("openxlsx")

library(duckdb)
library(DBI)
library(tidyverse)
library(openxlsx)


bucket <- Sys.getenv("WORKSPACE_BUCKET")
if (bucket == "") {
  bucket <- "bucket_placeholder"
}

drv <- duckdb(dbdir = "~/dbt.duckdb_eiv_6mo")
con <- dbConnect(drv)



dbExecute(con, "
  CREATE TABLE \"202609_src_meas_test3\" AS 
  SELECT * FROM read_csv(
    '../_study_data/consort_gira/eMERGE_6_Month_Data_External_Release/eMERGE_Measurement_Ex_Release_20260127.csv', 
    quote = '\"',
    decimal_separator = '.',
    nullstr = ['NA', 'NULL', 'N/A', 'null'],
    types = {'age_at_event': 'DOUBLE'} 
  )
")



dbGetQuery(con, 'DESCRIBE main."202609_src_meas_test3"')


aae_as_days <- dbGetQuery(con, '
SELECT
age_at_event
FROM main."202609_src_meas_test3"
WHERE SUBSTRING(cast(emerge_id as string), 1, 2) = 27
AND age_at_event
order by age_at_event desc
')

print(aae_as_days)
