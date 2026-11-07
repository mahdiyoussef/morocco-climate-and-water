{% macro generate_schema_name(custom_schema_name, node) -%}
  {{ custom_schema_name | trim if custom_schema_name else target.schema }}
{%- endmacro %}

{% test between(model, column_name, minimum, maximum) %}
select * from {{ model }}
where {{ column_name }} is null or {{ column_name }} < {{ minimum }} or {{ column_name }} > {{ maximum }}
{% endtest %}
