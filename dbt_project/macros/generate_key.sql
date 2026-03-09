{%- macro generate_key(domain_id,study_id,descriptor) -%}

    {%- set domain_id =
        '11' if domain_id == 'person' else
        '22' if domain_id == 'measurement' else
        '33' if domain_id == 'drug' else
        '44' if domain_id == 'procedure' else
        '55' if domain_id == 'device' else
        '00' -%}
    {%- set key_buffer =
        3000000 if study_id == 'consort_gira' else
        1000000 -%}
        CONCAT({{ domain_id }},{{ key_buffer }} + CAST({{ descriptor }} as integer)
        )
{%- endmacro -%}