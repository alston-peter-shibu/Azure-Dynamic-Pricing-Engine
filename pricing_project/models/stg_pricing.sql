SELECT
    city,
    demand AS demand_units,
    supply AS supply_units,
    CAST(event_time AS TIMESTAMP) AS event_time,
    ROUND(predicted_price, 2) AS predicted_price,
    ROUND(demand::FLOAT / supply, 2) AS demand_supply_ratio,
    EXTRACT(HOUR FROM event_time) AS event_hour,
    DAYOFWEEK(event_time) AS event_day_of_week
FROM {{ source('raw', 'pricing_stream') }}
WHERE demand IS NOT NULL
  AND supply IS NOT NULL
  AND supply > 0
