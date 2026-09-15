
    

    create  table
      "dbt"."dev_202609_omop"."observation_period__dbt_tmp"
  
    
    as (
      

with source as (

    select
    null::integer as "observation_period_id",
    null::integer as "person_id",
    null::text as "observation_period_start_date",
    null::text as "observation_period_end_date",
    null::integer as "period_type_concept_id"
    FROM "dbt"."dev_202609_vocab"."hidden"
    
)
select 
    * 
from source
limit 0
    );
    
  