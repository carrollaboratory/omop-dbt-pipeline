#!/usr/bin/env python
# ```
# Tool to query tables in the duckdb database.
# Create new cells for study specific analysis/validation. 
# ```

# +
import duckdb
import os
import pandas as pd

pd.set_option('display.max_colwidth', None)  # No limit (full content)
pd.set_option('display.max_rows', None)
# -

# Environment setup
if os.environ.get("WORKSPACE_BUCKET"):
    bucket = os.environ.get("WORKSPACE_BUCKET")
else:
    bucket = "bucket_placeholder"

engine = duckdb.connect("/tmp/dbt.duckdb")


def execute(query):
    """
    Connect to duckdb, execute a query and format as a DataFrame with headers.
    """
    result = engine.execute(query)
    try:
        return result.fetchdf()
    except Exception:
        return pd.DataFrame(result.fetchall(), columns=[col[0] for col in result.description])


# +
table = execute(
    """SELECT table_name FROM information_schema.tables 
    WHERE table_schema = 'main_main'
    --AND (table_name like '%measur%')
    --AND (table_name like '%icd%')
    --AND (table_name like '%src%')
    """
)
table

shapes = []
for t in table["table_name"]:
    nrows = execute(f'SELECT COUNT(*) AS nrows FROM "main_main"."{t}"').iloc[0]["nrows"]
    ncols = execute(f"""
        SELECT COUNT(*) AS ncols
        FROM information_schema.columns
        WHERE table_schema = 'main_main'
          AND table_name = '{t}'
    """).iloc[0]["ncols"]
    shapes.append({"table_name": t, "nrows": nrows, "ncols": ncols})

    
shape_df = pd.DataFrame(shapes).sort_values("table_name")
shape_df


# +
table = execute(
    "PRAGMA table_info('main_main.emerge_consort_gira_int_person_demographics')"
)

table
# -

# # analysis

table = execute(
    """
    SELECT 
    sum(case when withdrawal_status = 1 then 1 else 0 end) as '1_active',
    sum(case when withdrawal_status = 0 then 1 else 0 end) as '0_withdrawn',
    sum(case when (withdrawal_status not in (1, 0) or withdrawal_status is null) then 1 else 0 end) as 'unexpected_status'
    FROM main_main.emerge_consort_gira_int_person_demographics
    """
)
table

# # Measurement

# +
table = execute(
    """
    WITH concept_meas as (
    SELECT
        *
    FROM main_main.emerge_consort_gira_src_emerge_measurement_ex_release_20260127 meas_src
    LEFT JOIN (SELECT
               concept_id as mci_concept_id,
               concept_code as mci_concept_code,
               standard_concept as mci_standard_concept,
               vocabulary_id as mci_vocabulary_id,
               domain_id as mci_domain_id,
               concept_class_id as mci_concept_class_id
               FROM main_main.emerge_consort_gira_lookup_concepts 
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
group by mci_domain_id

    """
)
table

# measurement.measurement_concept_id grouped by domain_id
# -

table = execute(
    """


    SELECT
      range_low,
      COUNT(*) AS row_count
    FROM main_main.emerge_consort_gira_int_measurement_measurements
    WHERE range_low IS NOT NULL
    AND TRY_CAST(range_low AS INTEGER) IS NULL
    GROUP BY range_low
    ORDER BY row_count DESC, range_low;


    """
)
table
# TODO range_low should be castable to float --> Measurement table

table = execute(
    """

    SELECT
      range_high,
      COUNT(*) AS row_count
    FROM main_main.emerge_consort_gira_int_measurement_measurements
    WHERE range_high IS NOT NULL
    AND TRY_CAST(range_high AS INTEGER) IS NULL
    GROUP BY range_high
    ORDER BY row_count DESC, range_high;


    """
)
table
# TODO range_high should be castable to float --> Measurement table

# # Person

# +
table = execute(
    """

    SELECT 
    SUM(CASE WHEN year_of_birth is not null then 1 else 0 end) as yob_exists,
    SUM(CASE WHEN year_of_birth is null then 1 else 0 end) as yob_null
    FROM main_main.emerge_consort_gira_src_emerge_person_ex_release_20260123

    """
)
table

# yob is used for measurement.measurment_date. TODO: What to do when NULL
# +
# are all persons in measurements...etc in person table
# -

# # BMI

# +
table = execute(
    """
    WITH concept_meas as (
    SELECT
        *
    FROM main_main.emerge_consort_gira_src_emerge_bmi_ex_release_20260128 meas_src
    LEFT JOIN (SELECT
               concept_id as mci_concept_id,
               concept_code as mci_concept_code,
               standard_concept as mci_standard_concept,
               vocabulary_id as mci_vocabulary_id,
               domain_id as mci_domain_id,
               concept_class_id as mci_concept_class_id
               FROM main_main.emerge_consort_gira_lookup_concepts 
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
group by mci_domain_id

    """
)
table

# bmi.measurement_concept_id
# -
# # CPT

# +
table = execute(
    """
    
    WITH concept_meas as (
    SELECT
        *
    FROM main_main.emerge_consort_gira_src_emerge_cpt_ex_release_20260129 meas_src
    LEFT JOIN (SELECT
               concept_id as mci_concept_id,
               concept_code as mci_concept_code,
               standard_concept as mci_standard_concept,
               vocabulary_id as mci_vocabulary_id,
               domain_id as mci_domain_id,
               concept_class_id as mci_concept_class_id
               FROM main_main.emerge_consort_gira_lookup_concepts 
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
group by mci_domain_id

    """
)
table

# cpt.cpt_code

# -

# # ICD

table = execute(
    """
    WITH concept_icd as (
    SELECT
        *
    FROM main_main.emerge_consort_gira_src_emerge_icd_ex_release_20260129 icd_src
    LEFT JOIN (SELECT
               concept_id as ic_concept_id,
               concept_code as ic_concept_code,
               standard_concept as ic_standard_concept,
               vocabulary_id as ic_vocabulary_id,
               domain_id as ic_domain_id,
               concept_class_id as ic_concept_class_id
               FROM main_main.emerge_consort_gira_lookup_concepts 
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
        ic_concept_id
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
GROUP BY ic_domain_id
    """
)
table

# # vocabulary

# +
table = execute(
    """
    SELECT 
    distinct vocabulary_id
    FROM main_main.emerge_consort_gira_lookup_concepts c

     """
)
table

#  TODO Add tests to a 'src_data/concept_info' model(int?) to assert domains are expected as well as vocabularies.
#  EX: If the src measurement table is refreshed and now has a few procedures, or rows that don't join to the vocab at all.
# -

table = execute(
    """
    SELECT 
    sum(case when s_concept_id is null then 1 else 0 end) as n_null_standards
    ,sum(case when s_concept_id = 0 then 1 else 0 end) as n_invalid_standards
    ,sum(case when s_concept_id is not null and s_concept_id != 0 then 1 else 0 end) as n_valid_standards
    FROM main_main.emerge_consort_gira_lookup_standards s
    
     """
)
table

# +
table = execute(
    """
    
    WITH filtered_concept as (
    SELECT 
    distinct concept_id, concept_code, src_table, vocabulary_id, standard_concept, concept_id_1, concept_code_1
    FROM main_main.emerge_consort_gira_lookup_concepts c
    WHERE standard_concept IS NULL
    ),
    
    filtered_cr as (    
    SELECT src_table, vocabulary_id, standard_concept, concept_id_2
    FROM filtered_concept fc
    LEFT JOIN (SELECT * FROM main_main.CONCEPT_RELATIONSHIP WHERE relationship_id = 'Maps to') cr
    ON fc.concept_id_1 = cr.concept_id_1
    )
    
    SELECT
      src_table, vocabulary_id, standard_concept,
          SUM(CASE WHEN vocabulary_id IS NULL THEN 1 ELSE 0 END) as n_no_concept_join,
          SUM(CASE WHEN concept_id_2 IS NULL THEN 1 ELSE 0 END) as 'n_no_standard',

    FROM filtered_cr
    WHERE concept_id_2 IS NULL -- Filter out those that join to concept_relationship
    GROUP BY  src_table, vocabulary_id, standard_concept

    
    """
)
table

# The following are a subset of source codes/ids that are not Standard concepts.
# 1. Some don't join to the CONCEPT table at all. See column 'n_no_concept_join'
# 2. Some viable concepts don't have a mapped Standard concept 
# -

table = execute(
    """
    select * -- TODO make sure there are not more than one relationship 'Maps to'. Check for duplicates.
                   from main_main.CONCEPT_RELATIONSHIP
                   
                   where relationship_id = 'Maps to'  -- and invalid_reason is NULL
                   
""")
table


