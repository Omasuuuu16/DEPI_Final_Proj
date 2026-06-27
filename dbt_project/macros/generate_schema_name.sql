{# 
  Override the default schema name generation.
  Ensures custom schema names (staging, intermediate, marts, metrics)
  are used EXACTLY as specified — not prefixed with target.schema.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
