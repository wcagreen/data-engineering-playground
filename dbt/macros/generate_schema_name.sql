-- macros/generate_schema_name.sql
-- ---------------------------------
-- By default dbt prepends the target schema to custom schema names, producing
-- names like  production_staging  instead of just  staging.
-- This macro overrides that behaviour so the value in  +schema  is used as-is.

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
