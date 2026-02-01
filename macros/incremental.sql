{% macro incremental_where(load_ts_col) %}
  {% if is_incremental() %}
    where {{ load_ts_col }} >= (
      select coalesce(max({{ load_ts_col }}), '1900-01-01'::timestamp)
      from {{ this }}
    )
  {% endif %}
{% endmacro %}
