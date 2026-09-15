{%- macro generate_key(src_tbl,study_id,indexer) -%}

    {%- set src_tbl_id =
        '1' if src_tbl == 'bmi' else
        '2' if src_tbl == 'measurement' else
        '3' if src_tbl == 'cpt' else
        '4' if src_tbl == 'icd' else
        '5' if src_tbl == 'person' else
        '6' if src_tbl == 'combined' else
        '999' -%}

    {%- set key_buffer = 200000000 if study_id == 'consort_gira' else 100000000 -%}

    CAST({{ key_buffer }} AS INTEGER) + CAST(CONCAT('{{ src_tbl_id }}', {{ indexer }}) AS INTEGER)

{%- endmacro -%}