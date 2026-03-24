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


# table = execute(
#     "PRAGMA table_info('main_main.emerge_consort_gira_int_person_persons')"
# )
#
# table

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
# Make sure that joining a concept to 'Maps to value' as well as 'Maps to' will not cause an unexpectantly large number of new rows.

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
# -

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
SELECT distinct gender_concept_id , concept_name, care_site_name, count(emerge_id)
from
main_main.emerge_consort_gira_int_person_persons
left join main_omop.CONCEPT on gender_concept_id = concept_id
left join main_omop.care_site on substring(emerge_id, 1,2) = care_site_source_value
where concept_name is null or concept_name not in ('MALE','FEMALE')
group by gender_concept_id, concept_name, care_site_name
order by concept_name
"""
)

table


# +
table = execute(
"""

select distinct c.concept_id, c.concept_name as "cond_name"
from main_main.emerge_consort_gira_int_icd_conditions
left JOIN main_main.emerge_consort_gira_src_emerge_person_ex_release_20260123
using (emerge_id)
left join (select concept_id, concept_name, concept_code, vocabulary_id from main_omop.CONCEPT where domain_id = 'Condition') c
on s_condition_concept_id = c.concept_id
left join (select concept_id, concept_name from main_omop.CONCEPT where domain_id = 'Gender') g
on gender_concept_id = g.concept_id
left join (select person_id, gender_concept_id from main_omop.person) p
on emerge_id = person_id
where s_condition_concept_id in ('26935','193439','194420','194997','195500','196738','197032','197039','197237','197605','198197','198198','199078','200052','200670','200970','201072','201238','201257','201617','201909','433716','436155','436366','440971','443211','4150042','40482030','45772671')
order by concept_id


"""
)

table

# +
table = execute(
"""
select distinct icd_code,
icd_id,
s_condition_concept_id as standard_condition_concept_id,
vocabulary_id as "standard_vocab_id",
c.concept_name as "standard_cond_name",
g.concept_name as "src_gender",
care_site_name
from main_main.emerge_consort_gira_int_icd_conditions
left JOIN main_main.emerge_consort_gira_src_emerge_person_ex_release_20260123
using (emerge_id)
left join (select concept_id, concept_name, concept_code, vocabulary_id from main_omop.CONCEPT where domain_id = 'Condition') c
on s_condition_concept_id = c.concept_id
left join (select concept_id, concept_name from main_omop.CONCEPT where domain_id = 'Gender') g
on gender_concept_id = g.concept_id
left join (select person_id, gender_concept_id from main_omop.person) p
on emerge_id = person_id
left join main_omop.care_site on substring(emerge_id, 1,2) = care_site_source_value
where (s_condition_concept_id in ('193439','194420','195500','198198','199078','200052','201238','201257','201909','4150042') and src_gender != 'FEMALE')
   or (s_condition_concept_id in ('26935','194997','196738','197032','197039','197237','197605','198197','200670','201072','201617','433716','436155','436366','440971','443211','40482030','45772671') and src_gender != 'MALE')
order by g.concept_name

"""
)
table.to_csv('gender_condition_mismatch.csv')

table

# +

table = execute(
"""
with dist_persons as (
select emerge_id , 'b' as "table_id" from  main_main.emerge_consort_gira_src_emerge_bmi_ex_release_20260128 where age_at_event is null

union

select emerge_id , 'c' as "table_id" from  main_main.emerge_consort_gira_src_emerge_cpt_ex_release_20260129 where age_at_event is null

union 

select emerge_id, 'i' as "table_id"  from  main_main.emerge_consort_gira_src_emerge_icd_ex_release_20260129 where age_at_event is null

union 

select emerge_id, 'emerge_measurement_ex_release_20260127' as "table_id"  from  main_main.emerge_consort_gira_src_emerge_measurement_ex_release_20260127 where age_at_event is null
)
    
select 
table_id, care_site_name, count(emerge_id) as "n_records_missing_age_at_event"
from dist_persons
left join main_omop.care_site on substring(emerge_id, 1,2) = care_site_source_value
group by table_id, care_site_name, care_site_source_value

"""
)
table
# -

# # analysis

table = execute(
    """
    SELECT 
    care_site_name,
    sum(case when withdrawal_status = 1 then 1 else 0 end) as '1_active',
    sum(case when withdrawal_status = 0 then 1 else 0 end) as '0_withdrawn',
    sum(case when (withdrawal_status not in (1, 0) or withdrawal_status is null) then 1 else 0 end) as 'unexpected_status'
    FROM main_main.emerge_consort_gira_int_person_persons
    left join main_omop.care_site on substring(emerge_id, 1,2) = care_site_source_value
    group by care_site_name
    order by care_site_name
    
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
      range_low, care_site_name,
      COUNT(*) AS row_count
    FROM main_main.emerge_consort_gira_src_emerge_measurement_ex_release_20260127
    left join main_omop.care_site on substring(emerge_id, 1,2) = care_site_source_value

    WHERE range_low IS NOT NULL
    AND TRY_CAST(range_low AS float) IS NULL
    GROUP BY range_low, care_site_name
    ORDER BY row_count DESC, range_low;

    """
)
table
# TODO range_low should be castable to float --> Measurement table

table = execute(
    """

    SELECT
      range_high, care_site_name,
      COUNT(*) AS row_count
    FROM main_main.emerge_consort_gira_src_emerge_measurement_ex_release_20260127
    left join main_omop.care_site on substring(emerge_id, 1,2) = care_site_source_value
    WHERE range_high IS NOT NULL
    AND TRY_CAST(range_high AS float) IS NULL
    GROUP BY range_high, care_site_name
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
    count(emerge_id) as n_yob_null,
    care_site_name
    FROM main_main.emerge_consort_gira_src_emerge_person_ex_release_20260123
    left join main_omop.care_site on substring(emerge_id, 1,2) = care_site_source_value
    where year_of_birth is null
    group by care_site_name
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
select count(emerge_id), care_site_name
from dist_persons
left join main_omop.care_site on substring(emerge_id, 1,2) = care_site_source_value
where emerge_id not in (select distinct emerge_id from main_main.emerge_consort_gira_src_emerge_person_ex_release_20260123)
group by care_site_name
    """
)
table

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
select count(emerge_id) as "n_persons_in_person_tbl_only", care_site_name
from main_main.emerge_consort_gira_src_emerge_person_ex_release_20260123
left join main_omop.care_site on substring(emerge_id, 1,2) = care_site_source_value
where emerge_id not in (select emerge_id from dist_persons)
group by care_site_name
order by care_site_name


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
# +
table = execute(
    """
    WITH id_join_concepts as (
    SELECT substring(emerge_id, 1,2) as site_id, measurement_concept_id AS concept_id,
    null as concept_code,
    'M' as src_column,
    'measurement_concept_id' as field
    FROM main_main.emerge_consort_gira_src_emerge_measurement_ex_release_20260127
    WHERE measurement_concept_id IS NOT NULL

    UNION

    SELECT substring(emerge_id, 1,2) as site_id, unit_concept_id AS concept_id,
    unit_concept_as_text as concept_code,
    'M' as src_table,
    'unit_concept_id' as field
    FROM main_main.emerge_consort_gira_src_emerge_measurement_ex_release_20260127
    WHERE unit_concept_id IS NOT NULL
    
),

code_m_units_concepts as (
    SELECT substring(emerge_id, 1,2) as site_id, null as concept_id,
    unit_concept_as_text AS concept_code,
    'M' as src_table,
    'unit_concept_as_text' as field
    FROM main_main.emerge_consort_gira_src_emerge_measurement_ex_release_20260127
    WHERE unit_concept_id is null and unit_concept_as_text is not null
)

SELECT distinct * 
FROM id_join_concepts 
LEFT JOIN main_omop.CONCEPT AS concept
ON id_join_concepts.concept_id = concept.concept_id
where concept_name is null

union

SELECT distinct * 
FROM code_m_units_concepts 
LEFT JOIN main_omop.CONCEPT AS concept
ON code_m_units_concepts.concept_id = concept.concept_id
where concept_name is null

    
     """
)
# table.to_csv('meas_not_in_athena.csv')

table

# +
table = execute(
    """

    SELECT distinct gender_concept_id AS concept_id,
    'emerge_consort_gira_src_emerge_person_ex_release_20260123' as src_table,
    'gender' as "field",
    care_site_name
    FROM main_main.emerge_consort_gira_src_emerge_person_ex_release_20260123
    LEFT JOIN (select * from main_omop.CONCEPT where domain_id = 'Gender') AS g ON gender_concept_id = g.concept_id 
    left join main_omop.care_site on substring(emerge_id, 1,2) = care_site_source_value
    WHERE gender_concept_id IS NOT NULL
    and concept_name is null

    UNION

    SELECT distinct race_concept_id AS concept_id,
    'emerge_consort_gira_src_emerge_person_ex_release_20260123' as src_table,
    'race' as "field",
    care_site_name
    FROM main_main.emerge_consort_gira_src_emerge_person_ex_release_20260123
    LEFT JOIN (select * from main_omop.CONCEPT where domain_id = 'Race') AS g ON race_concept_id = g.concept_id 
    left join main_omop.care_site on substring(emerge_id, 1,2) = care_site_source_value
    WHERE race_concept_id IS NOT NULL
    and concept_name is null


    UNION

    SELECT distinct ethnicity_concept_id AS concept_id,
    'emerge_consort_gira_src_emerge_person_ex_release_20260123' as src_table,
    'ethnicity' as "field",
    care_site_name
    
    FROM main_main.emerge_consort_gira_src_emerge_person_ex_release_20260123
    LEFT JOIN (select * from main_omop.CONCEPT where domain_id = 'Ethnicity') AS g ON ethnicity_concept_id = g.concept_id 
    left join main_omop.care_site on substring(emerge_id, 1,2) = care_site_source_value
    WHERE ethnicity_concept_id IS NOT NULL
    and concept_name is null
    order by concept_id

    
     """
)
# table.to_csv('demographics_column_mismatch.csv')

table

# +
table = execute(
    """
   WITH id_join_concepts as (
   SELECT distinct measurement_concept_id AS src_concept_id,
    measurement_concept_name as src_concept_name,
    'BMI' as src_table,
    'measurement_concept_id' as "field",
    care_site_name
    FROM main_main.emerge_consort_gira_src_emerge_bmi_ex_release_20260128
    left join main_omop.care_site on substring(emerge_id, 1,2) = care_site_source_value
    WHERE measurement_concept_id IS NOT NULL

    UNION

    SELECT distinct unit_concept_id AS src_concept_id,
    unit_concept_name as src_concept_name,
    'BMI' as src_table,
    'unit_concept_id' as "field",
    care_site_name
    FROM main_main.emerge_consort_gira_src_emerge_bmi_ex_release_20260128
    left join main_omop.care_site on substring(emerge_id, 1,2) = care_site_source_value
    WHERE unit_concept_id IS NOT NULL
    
)

SELECT * 
FROM id_join_concepts 
LEFT JOIN main_omop.CONCEPT AS concept
ON id_join_concepts.src_concept_id = concept.concept_id
where concept.concept_code is null
   
     """
)
# table.to_csv('bmi_not_in_Athena.csv')

table


# +
table = execute(
    """
   WITH id_join_concepts as (


    SELECT distinct unit_concept_id AS src_concept_id,
    unit_concept_name as src_concept_name,
    'BMI' as src_table,
    'unit_concept_id' as "field",
    care_site_name
    FROM main_main.emerge_consort_gira_src_emerge_bmi_ex_release_20260128
    left join main_omop.care_site on substring(emerge_id, 1,2) = care_site_source_value
    WHERE unit_concept_id IS NOT NULL 
    and unit_concept_name = 'centimeter'
    
)

SELECT * 
FROM id_join_concepts 
LEFT JOIN main_omop.CONCEPT AS concept
ON id_join_concepts.src_concept_id = concept.concept_id
-- where concept.concept_code is null
order by src_concept_id, concept_name
     """
)
# table.to_csv('bmi_drill_down_centimeter.csv')

table
# -

table = execute(
    """
    with
code_concepts as (
    SELECT distinct
    icd_code AS concept_code,
    'emerge_consort_gira_src_emerge_icd_ex_release_20260129' as src_table,
    care_site_name
    FROM main_main.emerge_consort_gira_src_emerge_icd_ex_release_20260129
    left join main_omop.care_site on substring(emerge_id, 1,2) = care_site_source_value
    WHERE icd_code IS NOT NULL
)

SELECT code_concepts.*
FROM code_concepts 
LEFT JOIN main_omop.CONCEPT AS concept
ON code_concepts.concept_code = concept.concept_code 

AND vocabulary_id LIKE 'ICD%'
where concept_name is null
order by care_site_name
    
     """
)
table.to_csv('icd_code_not_in_athena_ICD.csv')
table

# +


table = execute(
    """
    with
code_concepts as (
    SELECT distinct
    cpt_code AS concept_code,
    'emerge_consort_gira_src_emerge_cpt_ex_release_20260129' as src_table,
    care_site_name
    FROM main_main.emerge_consort_gira_src_emerge_cpt_ex_release_20260129
    left join main_omop.care_site on substring(emerge_id, 1,2) = care_site_source_value
    WHERE cpt_code IS NOT NULL
)

SELECT code_concepts.*
FROM code_concepts 
LEFT JOIN main_omop.CONCEPT AS concept
ON code_concepts.concept_code = concept.concept_code 
AND vocabulary_id LIKE 'CPT4'
where concept_name is null
order by care_site_name
    
     """
)

# table.to_csv('cpt_code_not_in_athena_CPT.csv')

table
# -

table = execute(
    """
    with
code_concepts as (
    SELECT distinct
    unit_concept_id AS concept_code,
    'emerge_consort_gira_src_emerge_bmi_ex_release_20260128' as src_table,
    care_site_name
    FROM main_main.emerge_consort_gira_src_emerge_bmi_ex_release_20260128
    left join main_omop.care_site on substring(emerge_id, 1,2) = care_site_source_value
    WHERE unit_concept_id is not null and unit_concept_name is null
)

SELECT code_concepts.*
FROM code_concepts 
LEFT JOIN main_omop.CONCEPT AS concept
ON code_concepts.concept_code = concept.concept_code 
where concept_name is null
AND domain_id = 'Unit'
order by care_site_name
    
     """
)
table




