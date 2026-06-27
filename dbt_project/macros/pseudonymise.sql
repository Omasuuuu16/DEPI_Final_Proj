{#
  Pseudonymise a PII column using MD5 hashing.
  Usage: {{ pseudonymise_id('national_id') }}
         {{ pseudonymise_id('s.national_id') }}
#}
{% macro pseudonymise_id(column) %}
    md5(CAST({{ column }} AS VARCHAR))
{% endmacro %}
