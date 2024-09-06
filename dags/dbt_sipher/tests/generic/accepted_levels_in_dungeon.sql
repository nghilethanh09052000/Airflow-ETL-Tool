{% test accepted_levels_in_dungeon(model) %}
with exceptions as (
      SELECT
      *
        
      FROM {{model}}
      WHERE TRUE 
        AND IFNULL(gameplay_down,0) > 0 AND IFNULL(gameplay_hp_loss,0) = 0
      )

select * from exceptions

{% endtest %}
