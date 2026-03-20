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
    """
    SELECT 
    distinct drug_concept_id, drug_source_concept_id, domain_id , count(drug_source_concept_id) as "n_records"
    FROM main_omop.drug_exposure
    left join main_omop.CONCEPT 
    on drug_concept_id = concept_id
    where domain_id != 'Drug'
    group by drug_concept_id, drug_source_concept_id, domain_id 
limit 100
    """
)
print(table.shape)
print(table['n_records'].sum())
table

# https://athena.ohdsi.org/search-terms/terms?query=759727

# +
table = execute(
    """
    SELECT count(*), 'o' as "Table" from main_omop.observation
    union all 
    SELECT count(*), 'm' as "Table" from main_omop.measurement
    union all 
    SELECT count(*), 'c' as "Table" from main_omop.condition_occurrence   
    union all 
    SELECT count(*), 'p' as "Table" from main_omop.procedure_occurrence    
    union all 
    SELECT count(*), 'v' as "Table" from main_omop.visit_occurrence
    union all 
    SELECT count(*), 'p' as "Table" from main_omop.person   
    union all 
    SELECT count(*), 'v' as "Table" from main_omop.drug_exposure
    union all 
    SELECT count(*), 'v' as "Table" from main_omop.device_exposure    
    """
)
print(table.shape)
table

# https://athena.ohdsi.org/search-terms/terms?query=759727

# +
table = execute(
    """
    SELECT 
    distinct measurement_concept_id, measurement_source_concept_id, measurement_source_value, domain_id , count(measurement_source_concept_id) as "n_records"
    FROM main_omop.measurement
    left join main_omop.CONCEPT 
    on measurement_concept_id = concept_id
    where domain_id != 'Measurement'
    group by measurement_concept_id, measurement_source_concept_id, measurement_source_value, domain_id 
limit 100
    """
)
print(table.shape)
print(table['n_records'].sum())

table

# +
table = execute(
    """
    SELECT 
    distinct condition_concept_id, condition_source_concept_id, condition_source_value, domain_id , count(condition_source_concept_id) as "n_records"
    FROM main_omop.condition_occurrence
    left join main_omop.CONCEPT 
    on condition_concept_id = concept_id
    where domain_id != 'Condition'
    group by condition_concept_id, condition_source_concept_id, condition_source_value, domain_id 

    """
)
print(table.shape)
print(table['n_records'].sum())

table

# +
table = execute(
    """
    SELECT 
    distinct procedure_concept_id, procedure_source_concept_id, procedure_source_value, domain_id, count(procedure_source_concept_id) as "n_records"
    FROM main_omop.procedure_occurrence
    left join main_omop.CONCEPT 
    on procedure_concept_id = concept_id
    where domain_id != 'Procedure'
    group by procedure_concept_id, procedure_source_concept_id, procedure_source_value, domain_id
limit 100
    """
)
print(table.shape)
print(table['n_records'].sum())

table

# +
table = execute(
    """
    SELECT 
    distinct unit_concept_id, unit_source_concept_id,concept_id
    FROM main_omop.measurement
left join (select * from main_omop.CONCEPT where domain_id = 'Unit')
on unit_source_concept_id = concept_id
    where concept_id is not null
    limit 100
    """
)
print(table.shape)
table

# https://athena.ohdsi.org/search-terms/terms?query=759727
# -

table = execute(
    """
    select *
    from ( select * from main_main.emerge_consort_gira_src_emerge_bmi_ex_release_20260128 limit 500) src
join (select -- JOIN used to drop rows that are not domain 'Measurement'
      s_concept_id, s_concept_code, src_concept_id, domain_id
      from main_main.emerge_consort_gira_lookup_standards 
      where src_table = 'BMI'
      and domain_id = 'Measurement'
      and relationship_id = 'Maps to'
      ) as mci
    on src.measurement_concept_id = mci.src_concept_id
left join (select
      s_concept_id, s_concept_code, src_concept_id, src_concept_code
      from main_main.emerge_consort_gira_lookup_standards
      where src_table = 'BMI'
      and relationship_id = 'Maps to'
      ) as uci 
      on src.unit_concept_id = uci.src_concept_id
    
        """
)
print(table.shape)
table

# +
table = execute(
    """
    SELECT 
    distinct *
    FROM main_omop.measurement
    where value_as_concept_id is not null
    limit 100
    """
)
print(table.shape)
table

# https://athena.ohdsi.org/search-terms/terms?query=759727
# -

table = execute("""
    select concept_id, count(relationship_id)
    from main_omop.concept 
    join main_omop.concept_relationship on(concept_id=concept_id_1) 
    join (select distinct observation_source_concept_id from main_omop.observation) o on (concept_id=observation_source_concept_id) 
        group by concept_id, relationship_id
    
    having count(relationship_id) > 1 and  relationship_id='Maps to value'

    limit 100
      """
)
table  

table = execute("""
    SELECT 
    o.observation_source_concept_id,
    count(distinct r1.concept_id_2) * count(distinct r2.concept_id_2) AS potential_rows
    from (select distinct observation_source_concept_id from main_omop.observation) o
    join main_omop.concept_relationship r1 
        on o.observation_source_concept_id = r1.concept_id_1 
        and r1.relationship_id = 'Maps to'
    join main_omop.concept_relationship r2 
        on o.observation_source_concept_id = r2.concept_id_1 
        and r2.relationship_id = 'Maps to value'
    group by o.observation_source_concept_id
    order by count(distinct r1.concept_id_2) * count(distinct r2.concept_id_2) desc
    limit 100
      """
)
table 

table = execute("""
    select cr.*
    from main_omop.concept 
    join main_omop.concept_relationship cr on(concept_id=concept_id_1) 
    join (select distinct observation_source_concept_id from main_omop.observation) on (concept_id=observation_source_concept_id) 
    where relationship_id='Maps to value'
    and concept_id = 45537686
      """
)
table  

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
SELECT distinct gender_concept_id , concept_name
from
main_main.emerge_consort_gira_int_person_persons
left join main_omop.CONCEPT on gender_concept_id = concept_id

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

# +
# table = execute(
# """
# SELECT * FROM main_omop.drug_exposure
# WHERE drug_concept_id = 0
# limit 100
# """
# )
# 
# table

# table = execute(
# """
# SELECT * FROM main_omop.measurement
# WHERE measurement_date is null
# limit 100
# """
# )
# table

# table = execute(
# """
#     select
#     vo.birth_date,
#     meas.age_at_event,
#     date_add(vo.birth_date, INTERVAL (vo.age_at_event) YEAR)::date as "measurement_date"

#     from main_main.emerge_consort_gira_int_measurement_measurements as meas
#     left join main_main.emerge_consort_gira_int_visit_occurrences as vo
#     using (emerge_id, encounter_id)
#     where measurement_date is null
# """
# )
# table

table = execute(
"""
with dist_persons as (
select emerge_id , 'b' as "table" from  main_main.emerge_consort_gira_src_emerge_bmi_ex_release_20260128 where age_at_event is null

union

select emerge_id , 'c' as "table" from  main_main.emerge_consort_gira_src_emerge_cpt_ex_release_20260129 where age_at_event is null

union 

select emerge_id, 'i' as "table"  from  main_main.emerge_consort_gira_src_emerge_icd_ex_release_20260129 where age_at_event is null

union 

select emerge_id, 'm' as "table"  from  main_main.emerge_consort_gira_src_emerge_measurement_ex_release_20260127 where age_at_event is null
)
    
select * from dist_persons

"""
)
table
# -

table = execute(
"""
SELECT * FROM main_omop.visit_occurrence
WHERE visit_end_date is null
limit 100
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
# Added , null_padding=true to the models that read in the vocab tables. See this row was missing the last three values for some reason. 
# Patched in the pipeline. Should figure out what is going on with the download.
table = execute(
    """
    SELECT 
    *
    FROM main_omop.concept c
    where concept_name ilike '%pregabalin 25mg/1 / 50mg/1 / 75mg/1 / 100mg/1 / 150mg/1 / 200mg/1 / 225mg/1 / 300mg/1%'
    and valid_end_date is null
    
     """
)
table


# # Other


# +
#  Look into concepts that don't join to standards. breakdown by site_id
# -








