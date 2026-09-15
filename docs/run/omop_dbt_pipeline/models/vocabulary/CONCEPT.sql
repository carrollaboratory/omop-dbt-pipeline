
    

    create  table
      "dbt"."dev_202609_vocab"."CONCEPT__dbt_tmp"
  
    
    as (
      
        
SELECT
*
FROM read_csv('../../_study_data/vocabulary/20260318V2/CONCEPT.csv', AUTO_DETECT=TRUE, HEADER=TRUE, null_padding=true) -- todo Remove null_padding. Read errors at concept 1454005 which only has 7 of the 10 expected columns.
    );
    
  