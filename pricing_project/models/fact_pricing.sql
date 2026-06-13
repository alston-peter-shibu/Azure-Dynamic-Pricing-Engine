{{
  config(
    materialized='incremental',
    unique_key='event_time'
  )
}}

SELECT
    city,
    demand_units,
    supply_units,
    event_time,
    event_hour,
    event_day_of_week,
    predicted_price,
    demand_supply_ratio,
    CASE
        WHEN demand_supply_ratio > 1.5 THEN 'high_demand'
        WHEN demand_supply_ratio < 0.7 THEN 'low_demand'
        ELSE 'normal'
    END AS demand_flag
FROM {{ ref('stg_pricing') }}

{% if is_incremental() %}
WHERE event_time > (SELECT MAX(event_time) FROM {{ this }})
{% endif %}
