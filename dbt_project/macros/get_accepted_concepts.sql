{% macro get_accepted_concepts(table_id, column_id) %}
    {% set fk_info = run_query(
        "SELECT where_clause FROM " ~ ref('accepted_concepts') ~ " WHERE table = '" ~ table_id ~ "' AND column = '" ~ column_id ~ "'",
        execute=execute
    ) %}
    
    {% if execute %}
        {% if fk_info.rows %}
            {% set where_clause = fk_info.rows[0][0] %}
            SELECT DISTINCT *
            FROM {{ ref('CONCEPT') }}
            {{ where_clause }}
            AND standard_concept = 'S'
        {% else %}
            SELECT * FROM non_existent_table_will_cause_error_for_{{ table_id }}_{{ column_id }}
        {% endif %}
    {% else %}
        SELECT NULL as concept_id LIMIT 0
    {% endif %}
{% endmacro %}
