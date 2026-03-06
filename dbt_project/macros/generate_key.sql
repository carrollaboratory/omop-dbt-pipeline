{%- macro generate_key(src_tbl,tgt_col,study_id,row_id) -%}
{# 
src_tbl - used to differentiate a row from one source table from another. Use case: The first row of the src-table 'icd' and the first row in the src-table 'cpt' should generate different keys.

tgt_col - used to differentiate a row from one domain table from another. Use case: Keys for domain-table 'person' and the keys for domain-table 'Drug' should generate different keys.

study_id - For creating different_ids per study.

row_id - Necessary for each key to be unique and is expected to be a number/integer type
#}
    {%- set source_table_id =
        '11' if src_tbl == 'measurement' else
        '22' if src_tbl == 'person' else
        '33' if src_tbl == 'bmi' else
        '44' if src_tbl == 'icd' else
        '44' if src_tbl == 'cpt' else
        '00' -%}
    {%- set tgt_column_id =
        '11' if tgt_col == 'person_id' else
        '22' if tgt_col == 'measurement_id' else
        '33' if tgt_col == 'drug_id' else
        '44' if tgt_col == 'procedure_id' else
        '55' if tgt_col == 'device_exposure_id' else
        '00' -%}
    {%- set key_buffer =
        30000 if study_id == 'consort_gira' else
        10000 -%}
        CONCAT({{ source_table_id }},{{ tgt_column_id }},{{ key_buffer }} + CAST({{ row_id }} as integer))
{%- endmacro -%}