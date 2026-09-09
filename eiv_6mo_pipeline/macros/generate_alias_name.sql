{%- macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- if custom_alias_name is not none -%}
        {{- custom_alias_name -}}
    {%- elif node is not none and node.original_file_path.startswith('models/') -%}
        {%- set prefixes = [
            '20260902_tgt'
        ] -%}
        {%- set ns = namespace(alias=node.name) -%}
        {%- for prefix in prefixes -%}
            {%- if ns.alias.startswith(prefix) -%}
                {%- set ns.alias = ns.alias[prefix | length:] -%}
            {%- endif -%}
        {%- endfor -%}
        {{- ns.alias -}}
    {%- else -%}
        {{- node.name -}}
    {%- endif -%}
{%- endmacro -%}
