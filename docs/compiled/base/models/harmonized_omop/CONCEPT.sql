
        
SELECT
*
FROM read_csv('../../_study_data/vocabulary/CONCEPT.csv', AUTO_DETECT=TRUE, HEADER=TRUE, null_padding=true) -- todo Remove null_padding. Read errors at concept 1454005 which only has 7 of the 10 expected columns.