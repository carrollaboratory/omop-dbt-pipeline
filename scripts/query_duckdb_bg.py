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
    """SELECT table_name FROM information_schema.tables 
    WHERE table_schema = 'main_main'
    AND (table_name like '%observation%')
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

table = execute(
    """
    SELECT * 
    FROM main_main.emerge_consort_gira_src_emerge_cpt_ex_release_20260129

    where emerge_id = 6824934
    and cpt_code = '77334'
    
    """
)
table

table = execute(
    """
    SELECT count(*), device_exposure_id
    FROM main_main.emerge_consort_gira_stb_device_exposure
    group by device_exposure_id
    """
)
table

table = execute(
    """
    SELECT distinct *
    FROM main_main.emerge_consort_gira_stb_device_exposure
    where device_exposure_id = 203511056
    """
)
table

# +
# table = execute(
#     """
#     SELECT count(*), visit_occurrence_id
#     FROM main_main.emerge_consort_gira_int_visit_occurrences
#     group by visit_occurrence_id
#     order by count(*) desc
#     limit 10
#     """
# )
# table

table = execute(
    """
    SELECT *
    FROM main_main.emerge_consort_gira_int_visit_occurrences
    where visit_occurrence_id = 206181155
    """
)
table
# -

table = execute(
    """
    
with unioned as (
    select distinct emerge_id, encounter_id, age_at_event
    from main_main.emerge_consort_gira_src_emerge_measurement_ex_release_20260127
    where encounter_id = 40 and emerge_id = 3398642
    union
    select distinct emerge_id, encounter_id, age_at_event
    from main_main.emerge_consort_gira_src_emerge_bmi_ex_release_20260128
    where encounter_id = 40 and emerge_id = 3398642

    union
    select distinct emerge_id, encounter_id, age_at_event
    from main_main.emerge_consort_gira_src_emerge_cpt_ex_release_20260129
    where encounter_id = 40 and emerge_id = 3398642

    union
    select distinct emerge_id, encounter_id, age_at_event
    from main_main.emerge_consort_gira_src_emerge_icd_ex_release_20260129
    where encounter_id = 40 and emerge_id = 3398642

    
   ),
add_index as (
    select *, ROW_NUMBER() OVER () AS visit_index
    from unioned
    )
  
select *,
CAST(200000000 AS INTEGER) + CAST(CONCAT('6', visit_index) AS INTEGER)::integer as "visit_occurrence_id",from add_index
left join (select emerge_id, birth_date from main_main.emerge_consort_gira_int_person_persons )
using (emerge_id)

select *
from unioned
    """
)
table

table = execute(
    """
select emerge_id, birth_date from main_main.emerge_consort_gira_int_person_persons 
    where emerge_id = 3398642


    
    """
)
table

table = execute(
    """
with cleaned_race_ethnicity as (
    select
        emerge_id,
        s_concept_id,
        concept_value as race_ethnicity_concept_id,
        concept_type as original_column_flag,
        domain_id
    from "dbt"."main_main"."emerge_consort_gira_src_emerge_person_ex_release_20260123"
         unpivot (concept_value for concept_type in (race_concept_id, ethnicity_concept_id)) p
    left join (
        select
            s_concept_id, src_concept_id, s_concept_code, domain_id
        from "dbt"."main_main"."emerge_consort_gira_lookup_standards"
        where vocabulary_id in ('Race', 'Ethnicity')
    ) v
    on p.concept_value = v.src_concept_id
    where concept_value is not null
      and emerge_id = 3398642
)

select
    src.emerge_id,
    src.withdrawal_status,
    src.year_of_birth,
    case 
        when year_of_birth is not null then make_date(CAST(year_of_birth as integer), 6, 15)
        else make_date(1970, 6, 15)
    end as birth_date, -- Handle null year_of_birth
    src.gender_concept_id,
    v.s_concept_id as s_gender_concept_id,
    -- Aggregate to ensure one row per participant
    MAX(case when cre.domain_id = 'Race' then cre.s_concept_id end) as s_race_concept_id,
    MAX(case when cre.domain_id = 'Ethnicity' then cre.s_concept_id end) as s_ethnicity_concept_id
from "dbt"."main_main"."emerge_consort_gira_src_emerge_person_ex_release_20260123" src
left join cleaned_race_ethnicity cre
    on src.emerge_id = cre.emerge_id
    and cre.s_concept_id is not null
    and cre.domain_id in ('Race', 'Ethnicity')
left join (
    select s_concept_id, src_concept_id
    from "dbt"."main_main"."emerge_consort_gira_lookup_standards"
    where vocabulary_id in ('Gender')
) v
    on src.gender_concept_id = v.src_concept_id
where src.emerge_id = 3398642
group by
    src.emerge_id,
    src.withdrawal_status,
    src.year_of_birth,
    src.gender_concept_id,
    v.s_concept_id;

    
    """
)
table

table = execute(
    """
    SELECT distinct *
    FROM main_main.emerge_consort_gira_stb_device_exposure
    where person_id = 6824934
    --and device_concept_id = '77334'

    
    """
)
table

table = execute(
    """    
    SELECT
    emerge_id,
    src.age_at_event,
    cpt_code, -- direct from the source. Assume to be non-standard. 
    mci.src_concept_id as "cpt_id",
    mci.s_concept_id as "s_device_concept_id",
    mci.s_concept_code as "s_device_concept_code",
    row_id,
    encounter_id,
    gira_ror,
    src_index,
    FROM main_main.emerge_consort_gira_src_emerge_cpt_ex_release_20260129 src
    JOIN (SELECT
          s_concept_id, s_concept_code, src_concept_code, src_concept_id
          FROM main_main.emerge_consort_gira_lookup_standards
          WHERE src_table = 'CPT'
          AND domain_id = 'Device'
          ) AS mci
        ON src.cpt_code = mci.src_concept_code
    left join main_main.emerge_consort_gira_int_visit_occurrences as vo
    using (emerge_id, encounter_id)
    where emerge_id = 6824934
    and cpt_code = '77334'
        
            
    """
)
table

table = execute(
    """

select *
from main_main.emerge_consort_gira_int_visit_occurrences as vo
where emerge_id = 6824934
and encounter_id = 111
       
            
    """
)
table

# # analysis

table = execute(
    """


with all_emerge_ids as (
    select distinct emerge_id from main_main.emerge_consort_gira_src_emerge_person_ex_release_20260123
    union
    select distinct emerge_id from main_main.emerge_consort_gira_src_emerge_measurement_ex_release_20260127
    union
    select distinct emerge_id from main_main.emerge_consort_gira_src_emerge_bmi_ex_release_20260128
    union
    select distinct emerge_id from main_main.emerge_consort_gira_src_emerge_cpt_ex_release_20260129
    union
    select distinct emerge_id from main_main.emerge_consort_gira_src_emerge_icd_ex_release_20260129
)
,
unique_site_ids as (
    select distinct substring(emerge_id, 1, 2) as site_id
    from all_emerge_ids
    where emerge_id is not null
)

select
    site_id,
    m.site_name
from unique_site_ids u
left join {{ ref('site_key_mapping') }} m
    on u.site_id = m.site_key
    """
)
table

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

# +
table = execute(
    """
    WITH sample as (
    SELECT * 
    FROM main_main.emerge_consort_gira_src_emerge_cpt_ex_release_20260129 as t USING SAMPLE 1% 
    )
    
    SELECT DISTINCT domain_id as "cpt_domain_ids"
    FROM sample
    LEFT JOIN (SELECT 
    *
    FROM main_main.concept WHERE vocabulary_id = 'CPT4') as v
    ON sample.cpt_code = v.concept_code
    WHERE concept_id IS NOT NULL
    LIMIT 10
    
    """
)
table

# What are the domain_ids in the cpt data?
# -

table = execute(
    """
    WITH sample as (
    SELECT * 
    FROM main_main.emerge_consort_gira_src_emerge_icd_ex_release_20260129 as t USING SAMPLE 1% 
    )
    
    SELECT DISTINCT domain_id as "icd_domain_ids"
    FROM sample
    LEFT JOIN (SELECT 
    *
    FROM main_main.concept WHERE vocabulary_id LIKE 'ICD%') as v
    ON sample.icd_code = v.concept_code
    WHERE concept_id IS NOT NULL
    LIMIT 10
    
    """
)
table
# What are the domain_ids in the icd data?

# # Measurement

# +
table = execute(
    "PRAGMA table_info('main_main.emerge_consort_gira_int_measurement_measurements')"
)

table

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
WITH concept_bmi as (
    SELECT
    *
    FROM main_main.emerge_consort_gira_src_emerge_bmi_ex_release_20260128 bmi_src
    LEFT JOIN (select * from main_main.emerge_consort_gira_lookup_standards v where src_table = 'BMI') v
    ON bmi_src.measurement_concept_id = v.src_concept_id
)

    SELECT 
    domain_id, 
    SUM(CASE WHEN s_concept_id is not null and s_concept_id != 0 THEN 1 ELSE 0 END) AS rows_with_standard_concept,
    SUM(CASE WHEN s_concept_id is not null  and s_concept_id = 0 THEN 1 ELSE 0 END) AS rows_with_0_standard_concept,
    STRING_AGG(DISTINCT s_vocabulary_id, ', ') AS vocabulary_ids,
    STRING_AGG(DISTINCT domain_id, ', ') AS domain_ids,
    from concept_bmi
    GROUP BY domain_id

    """
)
table

# bmi.measurement_concept_id
# -
# # CPT

# +
table = execute(
    """
WITH concept_bmi as (
    SELECT
    *
    FROM main_main.emerge_consort_gira_src_emerge_cpt_ex_release_20260129 bmi_src
    LEFT JOIN (select * from main_main.emerge_consort_gira_lookup_standards v where src_table = 'CPT') v
    ON  = v.src_concept_id
)

    SELECT 
    domain_id, 
    SUM(CASE WHEN s_concept_id is not null and s_concept_id != 0 THEN 1 ELSE 0 END) AS rows_with_standard_concept,
    SUM(CASE WHEN s_concept_id is not null  and s_concept_id = 0 THEN 1 ELSE 0 END) AS rows_with_0_standard_concept,
    STRING_AGG(DISTINCT s_vocabulary_id, ', ') AS vocabulary_ids,
    STRING_AGG(DISTINCT domain_id, ', ') AS domain_ids,
    from concept_bmi
    GROUP BY domain_id

    """
)
table

# bmi.measurement_concept_id




# -

 table = execute(
    """

        select 
*
        from main_main.emerge_consort_gira_lookup_concepts c
where domain_id ='Observation'
and (concept_id_1 != '4245997' or concept_id_1 is null)
            """
)
table

 table = execute(
    """
    SELECT
    emerge_id,
    age_at_event,
    cpt_code,
    mci.src_concept_id as "cpt_id", -- concept_id for the cpt_code. This could be non-standard. 
    mci.s_concept_id as "s_observation_concept_id",
    mci.s_concept_code as "s_observation_concept_code",
    row_id,
    encounter_id,
    gira_ror,
    src_index,
    FROM main_main.emerge_consort_gira_src_emerge_cpt_ex_release_20260129 src
    LEFT JOIN (SELECT
          s_concept_id, s_concept_code, src_concept_code, src_concept_id
          FROM main_main.emerge_consort_gira_lookup_standards
          WHERE src_table = 'CPT'
          AND domain_id = 'Observation'
          ) AS mci
        ON src.cpt_code = mci.src_concept_code
    limit 100
            """
)
table


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
    SELECT *
    

    FROM main_main.emerge_consort_gira_lookup_standards s
where s_concept_id is null
    
     """
    
    cvx
    
)
table
# -

table = execute(
    """

    SELECT 
    *
    --sum(case when concept_id_1 is null then 1 else 0 end) as "n_null",
    --sum(case when concept_id_1 is not null then 1 else 0 end) as "n_not"

    FROM main_main.emerge_consort_gira_lookup_concepts s
    where concept_id_1 is null

    
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
    
    WITH 
    
    filtered_concept as (
    SELECT 
    distinct concept_id as src_concept_id, 
    concept_code as src_concept_code,
    src_table,
    vocabulary_id,
    domain_id
    FROM main_main.emerge_consort_gira_lookup_concepts c
    ),
    
    join_cr as (
    SELECT  
    distinct src_concept_id, 
    src_concept_code,
    src_table,
    vocabulary_id,
    domain_id,
    concept_id_2 as "s_concept_id",
    FROM filtered_concept fc
    LEFT JOIN (SELECT * FROM main_main.CONCEPT_RELATIONSHIP WHERE relationship_id = 'Maps to' AND invalid_reason IS NULL) cr
    ON fc.src_concept_id = cr.concept_id_1
    )
    
    SELECT    
    distinct src_concept_id, 
    src_concept_code,
    src_table,
    jcr.vocabulary_id,
    jcr.domain_id,
    s_concept_id,
    base_concept.concept_code as "s_concept_code",
    base_concept.standard_concept as 's_standard_concept',
    base_concept.vocabulary_id as 's_vocabulary_id'
    FROM join_cr jcr
    LEFT JOIN main_main.CONCEPT base_concept
    ON jcr.s_concept_id = base_concept.concept_id


    """
)
table

table = execute(
    """
        with 
    
    filtered_concept as (
        select 
        distinct concept_id as src_concept_id, 
        concept_code as src_concept_code,
        concept_id_1 as c_concept_id,
        src_table,
        vocabulary_id,
        domain_id
        from main_main.emerge_consort_gira_lookup_concepts
    ),
    
    jcr as (
        select  
        distinct src_concept_id, 
        src_concept_code,
        src_table,
        vocabulary_id,
        domain_id,
        concept_id_2
        from filtered_concept fc
        left join (select * -- TODO make sure there are not more than one relationship 'Maps to'. Check for duplicates.
                   from main_main.CONCEPT_RELATIONSHIP
                   where relationship_id = 'Maps to'  -- and invalid_reason is NULL
                  ) cr
        on fc.c_concept_id = cr.concept_id_1
    )
    
    select    
    distinct src_concept_id, 
    src_concept_code,
    src_table,
    jcr.vocabulary_id,
    jcr.domain_id,
      case 
      when concept_id_2 is not null and valid_end_date = '20991231' then concept_id_2
      -- A standard concept exists and is valid
      when valid_end_date != '20991231' then 0
      -- A standard concept exists but it is invalid
      else null
      -- The Src-cid didn't join to CONCEPT.
      -- The Src-cid didn't join to CONCEPT_RELATIONSHIP.
    end as "s_concept_id",
      case 
      when concept_id_2 is not null and valid_end_date = '20991231' then base_concept.concept_code
      -- A standard concept exists and is valid
      else null      
      -- A standard concept exists but it is invalid      
      -- The Src-cid didn't join to CONCEPT.
      -- The Src-cid didn't join to CONCEPT_RELATIONSHIP.
    end as "s_concept_code",
    base_concept.vocabulary_id as 's_vocabulary_id'
    from jcr
    left join (select cast(valid_end_date as string) as valid_end_date, concept_id, concept_code , vocabulary_id
               from main_main.CONCEPT
               ) as base_concept
    on jcr.concept_id_2 = base_concept.concept_id
    
    
    """
)
table

table = execute(
    """
    select * -- TODO make sure there are not more than one relationship 'Maps to'. Check for duplicates.
                   from main_main.CONCEPT_RELATIONSHIP
                   
                   where relationship_id = 'Maps to'  -- and invalid_reason is NULL
                   
""")
table

table = execute(
    """
    
    with code_cpt_concepts as (
    SELECT DISTINCT null as concept_id,
    cpt_code AS concept_code,
    'CPT' as src_table
    FROM main_main.emerge_consort_gira_src_emerge_cpt_ex_release_20260129
    WHERE cpt_code IS NOT NULL
)

SELECT * 
FROM code_cpt_concepts ccc
LEFT JOIN main_main.CONCEPT AS concept
ON ccc.concept_code = concept.concept_code 
WHERE vocabulary_id = 'CPT4'
and ccc.concept_code = '94664'

""")
table

table = execute(
    """

SELECT * 
FROM main_main.emerge_consort_gira_lookup_concepts AS concept
WHERE vocabulary_id = 'CPT4'
and concept_code = '94664'

""")
table

table = execute(
    """
    
    SELECT 
*
    FROM main_main.concept c
    where concept_code = 'C8908'
    --where vocabulary_id = 'CPT4'

    """
)
table


