{% macro add_audit_columns() %}
  '{{ invocation_id }}' as dbt_invocation_id
{% endmacro %}
