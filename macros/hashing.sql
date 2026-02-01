{% macro hash_key(columns) %}
  md5(
    concat_ws('||'
      {%- for c in columns -%}
        , coalesce(cast({{ c }} as varchar), '')
      {%- endfor -%}
    )
  )
{% endmacro %}
