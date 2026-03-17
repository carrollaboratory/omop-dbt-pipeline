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

engine = duckdb.connect("~/dbt.duckdb")


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
    WHERE table_schema like 'main_main'
    --AND (table_name like '%stb%' OR table_name like '%int%')
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
    """SELECT table_name FROM information_schema.tables 
    WHERE table_schema like '%omop%'
    --AND (table_name like '%stb%' OR table_name like '%int%')
    """
)
table

shapes = []
for t in table["table_name"]:
    nrows = execute(f'SELECT COUNT(*) AS nrows FROM "main_omop"."{t}"').iloc[0]["nrows"]
    ncols = execute(f"""
        SELECT COUNT(*) AS ncols
        FROM information_schema.columns
        WHERE table_schema = 'main_omop'
          AND table_name = '{t}'
    """).iloc[0]["ncols"]
    shapes.append({"table_name": t, "nrows": nrows, "ncols": ncols})

    
shape_df = pd.DataFrame(shapes).sort_values("table_name")
shape_df


# +
table = execute(
"""
SELECT distinct condition_concept_id,c.concept_code as 'cond_code', c.concept_name as "cond_name", gender_concept_id, g.concept_name as "gender_concept_name"
FROM main_omop.condition_occurrence
left JOIN main_omop.person
using (person_id)
left join (select concept_id, concept_name, concept_code from main_omop.CONCEPT where domain_id = 'Condition') c
on condition_concept_id = c.concept_id
left join (select concept_id, concept_name from main_omop.CONCEPT where domain_id = 'Gender') g
on gender_concept_id = g.concept_id
where condition_concept_id in ('201617','198198','197237','198197','197039','4150042','440971','201257','200052','199078')
order by c.concept_name
"""
)

table

# +
table = execute(
"""

select distinct icd_code, icd_id, s_condition_concept_id, vocabulary_id as "s_vocab_id", c.concept_name as "cond_name", g.concept_name as "src_gender", g.concept_id as "src_c_gender", p.gender_concept_id as "omop_gender"
from main_main.emerge_consort_gira_int_icd_conditions
left JOIN main_main.emerge_consort_gira_src_emerge_person_ex_release_20260123
using (emerge_id)
left join (select concept_id, concept_name, concept_code, vocabulary_id from main_omop.CONCEPT where domain_id = 'Condition') c
on s_condition_concept_id = c.concept_id
left join (select concept_id, concept_name from main_omop.CONCEPT where domain_id = 'Gender') g
on gender_concept_id = g.concept_id
left join (select person_id, gender_concept_id from main_omop.person) p
on emerge_id = person_id
where s_condition_concept_id in ('201617','198198','197237','198197','197039','4150042','440971','201257','200052','199078')
order by icd_code, g.concept_name
limit 50


"""
)

table
# -

# table = execute(
# """
# select distinct gender_concept_id, s_gender_concept_id, concept_name from main_main.emerge_consort_gira_int_person_persons p
# left join (select concept_id, concept_name from main_omop.CONCEPT where domain_id = 'Gender') g
# on gender_concept_id = g.concept_id
# """
# )
# table
table = execute(
"""
select condition_occurrence_id, count(condition_occurrence_id) from main_omop.condition_occurrence
group by condition_occurrence_id
having count(condition_occurrence_id) > 1
limit 10


"""
)
table

table = execute(
"""
--SELECT * FROM main_omop.CONCEPT WHERE vocabulary_id = 'CDM' AND concept_class_id = 'CDM'
SELECT vocabulary_version from main_omop.vocabulary  where vocabulary_id = 'None'

"""
)
table

# +
# table = execute(
# """
# SELECT *
# FROM main_main.emerge_consort_gira_lookup_concepts
# where concept_code = '0144T'
# limit 100
# """
# )

# table

# table = execute(
# """
# SELECT *
# FROM main_main.emerge_consort_gira_lookup_standards
# where src_concept_code = '0144T'
# limit 100
# """
# )

# table

table = execute(
"""
SELECT *
FROM main_main.emerge_consort_gira_int_cpt_none
--where src_concept_code = '0144T'
limit 100
"""
)

table
# -

# table = execute(
#     "PRAGMA table_info('main_main.emerge_consort_gira_int_person_persons')"
# )
#
# table

# # analysis

table = execute(
    """
    SELECT 
    sum(case when withdrawal_status = 1 then 1 else 0 end) as '1_active',
    sum(case when withdrawal_status = 0 then 1 else 0 end) as '0_withdrawn',
    sum(case when (withdrawal_status not in (1, 0) or withdrawal_status is null) then 1 else 0 end) as 'unexpected_status'
    FROM main_main.emerge_consort_gira_int_person_persons
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

# src measurement.measurement_concept_id joined to CONCEPT grouped by domain_id
# -

table = execute(
    """


    SELECT
      range_low,
      COUNT(*) AS row_count
    FROM main_main.emerge_consort_gira_src_emerge_measurement_ex_release_20260127
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
    FROM main_main.emerge_consort_gira_src_emerge_measurement_ex_release_20260127
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

table = execute(
    """
with dist_persons as (
select distinct emerge_id from  main_main.emerge_consort_gira_src_emerge_bmi_ex_release_20260128

union

select distinct emerge_id from  main_main.emerge_consort_gira_src_emerge_cpt_ex_release_20260129

union 

select distinct emerge_id from  main_main.emerge_consort_gira_src_emerge_icd_ex_release_20260129

union 

select distinct emerge_id from  main_main.emerge_consort_gira_src_emerge_measurement_ex_release_20260127
)
select emerge_id
from dist_persons
where emerge_id not in (select distinct emerge_id from main_main.emerge_consort_gira_src_emerge_person_ex_release_20260123)
    """
)
table

# +
# are all persons in measurements...etc in person table

table = execute(
    """
select count( *) 
from 
--main_main.emerge_consort_gira_src_emerge_bmi_ex_release_20260128
--main_main.emerge_consort_gira_src_emerge_cpt_ex_release_20260129
--main_main.emerge_consort_gira_src_emerge_icd_ex_release_20260129
main_main.emerge_consort_gira_src_emerge_measurement_ex_release_20260127
where emerge_id = 3343983

    """
)
table

# bmi(252) cpt(111) icd(402) meas(166)

# +
# are all persons in measurements...etc in person table

table = execute(
    """
with dist_persons as (
select distinct emerge_id from  main_main.emerge_consort_gira_src_emerge_bmi_ex_release_20260128

union

select distinct emerge_id from  main_main.emerge_consort_gira_src_emerge_cpt_ex_release_20260129

union 

select distinct emerge_id from  main_main.emerge_consort_gira_src_emerge_icd_ex_release_20260129

union 

select distinct emerge_id from  main_main.emerge_consort_gira_src_emerge_measurement_ex_release_20260127
)
select count(emerge_id) as "n_persons_in_person_tbl_only", dbgap_site_name, withdrawal_status
from main_main.emerge_consort_gira_src_emerge_person_ex_release_20260123
left join main.care_site_seed
on substring(emerge_id, 1,2) = dbgap_site_id
where emerge_id not in (select emerge_id from dist_persons)
group by dbgap_site_name, withdrawal_status

    """
)
table

# +
# are all persons in measurements...etc in person table

table = execute(
    """



select *
from 
--main_main.emerge_consort_gira_src_emerge_bmi_ex_release_20260128
--main_main.emerge_consort_gira_src_emerge_cpt_ex_release_20260129
--main_main.emerge_consort_gira_src_emerge_icd_ex_release_20260129
--main_main.emerge_consort_gira_src_emerge_measurement_ex_release_20260127
main_main.emerge_consort_gira_src_emerge_person_ex_release_20260123
where emerge_id = 3343983

    """
)
table
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

# src bmi.measurement_concept_id joined to CONCEPT grouped by domain_id
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

# src cpt.cpt_code joined to CONCEPT grouped by domain_id
# -

# # ICD

# +
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

# src icd.icd_code joined to CONCEPT grouped by domain_id
# -

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
# # Other


# +
#  Look into concepts that don't join to standards. breakdown by site_id
# -








