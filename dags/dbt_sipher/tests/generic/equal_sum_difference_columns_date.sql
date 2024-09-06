{% test equal_sum_difference_columns_date(model, model_2, column_1, column_2, column_date, day) %}
--- To test whether the sum of columns from table_1 is equal to the sum of columns from table_2 
WITH table_1 AS (
    SELECT
    SUM({{column_1}}) AS {{column_1}}
    FROM {{model}}
    WHERE TRUE 
)
, table_2 AS (
    SELECT
    SUM({{column_2}}) AS {{column_2}}
    FROM {{model_2}}
    WHERE TRUE
        AND {{column_date}} <= (SELECT MAX({{column_date}}) - INTERVAL {{day}} DAY FROM {{model}})
)
, _except AS (
    SELECT
    (table_1.{{column_1}} - table_2.{{column_2}}) AS error
    FROM
    table_1, table_2
)

SELECT * FROM _except 
WHERE 
    error > 0.0001 OR error < -0.0001
{% endtest %}