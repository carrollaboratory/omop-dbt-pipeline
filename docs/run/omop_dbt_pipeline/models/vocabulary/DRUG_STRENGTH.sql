
    

    create  table
      "dbt"."dev_202609_vocab"."DRUG_STRENGTH__dbt_tmp"
  
    
    as (
      
        
SELECT
*
FROM read_csv('../../_study_data/vocabulary/20260318V2/DRUG_STRENGTH.csv', AUTO_DETECT=TRUE, HEADER=TRUE)
    );
    
  