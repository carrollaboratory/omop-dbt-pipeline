
    

    create  table
      "dbt"."dev_202609_vocab"."CONCEPT_ANCESTOR__dbt_tmp"
  
    
    as (
      
        
SELECT
*
FROM read_csv('../../_study_data/vocabulary/20260318V2/CONCEPT_ANCESTOR.csv', AUTO_DETECT=TRUE, HEADER=TRUE, null_padding=true)
    );
    
  