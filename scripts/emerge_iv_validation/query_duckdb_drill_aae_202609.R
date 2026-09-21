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

aae_as_years <- execute(
  "
  SELECT
      site_name,
      COUNT(*) AS total_count,
      COUNT(*) FILTER (WHERE aae IS NULL) AS is_null,
      COUNT(*) FILTER (WHERE aae = 0) AS is_zero,
      COUNT(*) FILTER (WHERE aae > 0 AND aae <= 90) AS one_to_ninety,
      COUNT(*) FILTER (WHERE aae > 90 AND aae <= 120) AS ninety_to_onetwenty,
      COUNT(*) FILTER (WHERE aae > 120) AS more_than_onetwenty,

      SUM(has_decimal) AS source_was_float,
      SUM(has_integer) AS source_was_int

  FROM (
      SELECT
          *,
          CAST(age_at_event AS INTEGER) AS aae,

          CASE
              WHEN CAST(age_at_event as float) IS NOT NULL
                   AND CAST(age_at_event as float) != FLOOR(CAST(age_at_event as integer))
              THEN 1
              ELSE 0
          END AS has_decimal,

          CASE
              WHEN CAST(age_at_event as float) IS NOT NULL
                   AND CAST(age_at_event as float) = FLOOR(CAST(age_at_event as float))
              THEN 1
              ELSE 0
          END AS has_integer

      FROM dev_202609_6mo_src.emerge_consort_gira_src_emerge_measurement_ex_release_20260127
  ) AS p

  LEFT JOIN dev_202609_6mo_int.emerge_consort_gira_int_care_sites s
      ON SUBSTRING(emerge_id, 1, 2) = s.site_id

  GROUP BY site_name
  ORDER BY site_name;
  "
)

print(aae_as_years)




aae_as_days <- execute(
  "
  SELECT
      site_name,
      COUNT(*) AS total_count,
      COUNT(*) FILTER (WHERE aae IS NULL) AS is_null,
      COUNT(*) FILTER (WHERE aae BETWEEN 0 and 25) AS one_to_twentyfive,
      COUNT(*) FILTER (WHERE aae > 25 AND aae <= 50) AS twentyfive_to_fifty,
      COUNT(*) FILTER (WHERE aae > 50 AND aae <= 75) AS fifty_to_seventyfive,
      COUNT(*) FILTER (WHERE aae > 75 AND aae <= 90) AS seventyfive_to_ninety,
      COUNT(*) FILTER (WHERE aae > 90 AND aae <= 120) AS ninety_to_onetwenty,
      COUNT(*) FILTER (WHERE aae > 120) AS more_than_onetwenty

  FROM (
      SELECT
          *,
          CAST(age_at_event AS INTEGER) AS aae


      --FROM dev_202609_6mo_src.emerge_consort_gira_src_emerge_measurement_ex_release_20260127
      --FROM dev_202609_6mo_src.emerge_consort_gira_src_emerge_icd_ex_release_20260129
      --FROM dev_202609_6mo_src.emerge_consort_gira_src_emerge_cpt_ex_release_20260129
      FROM dev_202609_6mo_src.emerge_consort_gira_src_emerge_bmi_ex_release_20260128


  ) AS p

  LEFT JOIN dev_202609_6mo_int.emerge_consort_gira_int_care_sites s
      ON SUBSTRING(emerge_id, 1, 2) = s.site_id

  GROUP BY site_name
  ORDER BY site_name;
  "
)

print(aae_as_days)




table_names <- execute(
  "
  SELECT
      site_name,
      AVG(CAST(age_at_event AS FLOAT)) AS avg_aae,
      AVG(CAST(age_at_event  AS FLOAT) / 365)  AS avg_aae_divide_by_365

      -- FROM dev_202609_6mo_src.emerge_consort_gira_src_emerge_measurement_ex_release_20260127
      -- FROM dev_202609_6mo_src.emerge_consort_gira_src_emerge_icd_ex_release_20260129
      -- FROM dev_202609_6mo_src.emerge_consort_gira_src_emerge_cpt_ex_release_20260129
      FROM dev_202609_6mo_src.emerge_consort_gira_src_emerge_bmi_ex_release_20260128

  LEFT JOIN dev_202609_6mo_int.emerge_consort_gira_int_care_sites s
      ON SUBSTRING(emerge_id, 1, 2) = s.site_id

  GROUP BY site_name
  ORDER BY site_name
  "
)

print(table_names)





table_names <- execute(
  "
  SELECT
distinct age_at_event
  FROM dev_202609_6mo_src.emerge_consort_gira_src_emerge_measurement_ex_release_20260127

  LEFT JOIN dev_202609_6mo_int.emerge_consort_gira_int_care_sites s
      ON SUBSTRING(emerge_id, 1, 2) = s.site_id
  where site_name = 'Vanderbilt University'

  "
)

print(table_names)






aae_as_days <- execute(
  "
  SELECT
      site_name,
      COUNT(*) AS total_count,
      COUNT(*) FILTER (WHERE aae < 120) AS less_than_onetwenty,
      COUNT(*) FILTER (WHERE aae between 120 and 1000) AS onetwenty_to_thousand,
      COUNT(*) FILTER (WHERE aae between 1000 and 5000) AS thousand_to_fivethousand,
      COUNT(*) FILTER (WHERE aae > 7000) AS more_than_seventhousand,
  FROM (
      SELECT
          *,
          CAST(age_at_event AS INTEGER) AS aae
      FROM dev_202609_6mo_src.emerge_consort_gira_src_emerge_measurement_ex_release_20260127
  ) AS p

  LEFT JOIN dev_202609_6mo_int.emerge_consort_gira_int_care_sites s
      ON SUBSTRING(emerge_id, 1, 2) = s.site_id
  where site_name = 'Vanderbilt University'

  GROUP BY site_name
  ORDER BY site_name;
  "
)

print(aae_as_days)









mm_aae <- execute(
  "SELECT distinct age_at_event FROM dev_202609_6mo_int.emerge_consort_gira_int_measurement_measurements

  ")
print(mm_aae)



src_age <- execute(
  "SELECT distinct year_of_birth FROM dev_202609_6mo_src.emerge_consort_gira_src_emerge_person_ex_release_20260123

  ")
print(src_age)




view_aae <- execute(
  "SELECT 
  -- distinct(x_age_at_event)
  measurement_date
  FROM dev_202609_omop.measurement
  order by 1 desc
  ")
print(view_aae)




table_names <- execute(
  "
  with meas as (SELECT
      'omop_measurement' as table,
      measurement_date
      FROM dev_202609_omop.measurement
  LEFT JOIN dev_202609_6mo_int.emerge_consort_gira_int_care_sites s
      ON SUBSTRING(cast(person_id as string), 1, 2) = s.site_id
  where site_name = 'Vanderbilt University'
and measurement_date > CURRENT_DATE
limit 5)
, obs as (
  SELECT
  'omop_observation' as table,
  observation_date
  FROM dev_202609_omop.observation
  LEFT JOIN dev_202609_6mo_int.emerge_consort_gira_int_care_sites s
  ON SUBSTRING(cast(person_id as string), 1, 2) = s.site_id
  where site_name = 'Vanderbilt University'
  and observation_date > CURRENT_DATE
  limit 5 )
  
  select * from meas
  union
  select * from obs
")

print(table_names)



aae_as_days <- execute(
  "
  SELECT
      site_name,
      COUNT(*) AS total_count,
      COUNT(*) FILTER (WHERE aae < 120) AS less_than_onetwenty,
      COUNT(*) FILTER (WHERE aae between 120 and 1000) AS onetwenty_to_thousand,
      COUNT(*) FILTER (WHERE aae between 1000 and 5000) AS thousand_to_fivethousand,
      COUNT(*) FILTER (WHERE aae > 7000) AS more_than_seventhousand,
  FROM (
      SELECT
          *,
          CAST(age_at_event AS INTEGER) AS aae
      FROM dev_202609_6mo_src.emerge_consort_gira_src_emerge_measurement_ex_release_20260127
  ) AS p

  LEFT JOIN dev_202609_6mo_int.emerge_consort_gira_int_care_sites s
      ON SUBSTRING(emerge_id, 1, 2) = s.site_id
  where site_name = 'Vanderbilt University'

  GROUP BY site_name
  ORDER BY site_name;
  "
)

print(aae_as_days)


