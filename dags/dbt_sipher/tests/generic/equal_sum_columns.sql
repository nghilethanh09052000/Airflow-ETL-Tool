{% test equal_sum_columns(model, model_2, columns) %}
--- To test whether the sum of columns from table_1 is equal to the sum of columns from table_2
WITH table_1 AS (
    SELECT
    {%- for column in columns %}
    SUM({{column}}) AS {{column}}
    {{ "," if not loop.last }}
    {%- endfor %}
    FROM {{model}}
    WHERE TRUE 
)
, table_2 AS (
    SELECT
    {%- for column in columns %}
    SUM({{column}}) AS {{column}}
    {{ "," if not loop.last }}
    {%- endfor %}
    FROM {{model_2}}
    WHERE TRUE
)
, _except AS (
    SELECT
    {%- for column in columns %}
    (table_1.{{column}} - table_2.{{column}}) AS {{column}}
    {{ "," if not loop.last }}
    {%- endfor %}
    FROM
    table_1, table_2
)

SELECT * FROM _except 
WHERE 
{%- for column in columns %}
{{column}} > 0.0001 OR {{column}} < -0.0001
{{ "AND" if not loop.last }}
{%- endfor %}

{% endtest %}