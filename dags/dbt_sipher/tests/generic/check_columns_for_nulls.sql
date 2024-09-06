{% test check_columns_for_nulls(model, columns, condition ) %}
with exceptions as (
      SELECT
      COUNT(*) AS error_count
        
      FROM {{model}}
      WHERE TRUE 
      AND {{condition}}
      AND  {%- for column in columns %}

      {{column}} IS NULL  
      {{ "AND" if not loop.last }}

        {%- endfor %}
      )

select * from exceptions
where error_count > 0

{% endtest %}
