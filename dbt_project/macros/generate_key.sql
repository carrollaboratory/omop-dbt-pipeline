{%- macro generate_key(src_tbl,study_id,indexer) -%}

    {%- set src_tbl_id =
        '11' if src_tbl == 'bmi' else
        '22' if src_tbl == 'measurement' else
        '33' if src_tbl == 'cpt' else
        '44' if src_tbl == 'icd' else
        '55' if src_tbl == 'person' else
        '00' -%}

    {%- set key_buffer = 3000000 if study_id == 'consort_gira' else 1000000 -%}

    CAST({{ key_buffer }} AS INTEGER) + CAST(CONCAT('{{ src_tbl_id }}', {{ indexer }}) AS INTEGER)

{%- endmacro -%}