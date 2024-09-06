{% test hploss_non_zero_when_down_nonzero(model,hp_column, down_column) %}
with exceptions as (
      SELECT
              *        
      FROM {{model}}
      WHERE TRUE 
        AND IFNULL({{down_column}},0) > 0 AND IFNULL({{hp_column}},0) = 0
      )

select * from exceptions

{% endtest %}
