--
SELECT
    DATE_TRUNC('month', pickup_datetime) as month,
AVG(trip_distance) as avg_trip_distance,
STDDEV(trip_distance) as stddev_trip_distance
FROM "nyc-trip-data_refined"."trip_data"
WHERE
    {{source}}
  AND trip_distance <= 30
GROUP BY 1
ORDER BY 1 ASC

--
SELECT
    source,
    DATE_TRUNC('month', pickup_datetime) as month,
COUNT(*) as total_trips
FROM "nyc-trip-data_refined"."trip_data"
WHERE
    {{source}}
GROUP BY 1,2
ORDER BY 2,1 ASC

--
SELECT
    CASE
        WHEN ROUND(payment_type, 0) = 1 THEN 'CREDIT-CARD'
        WHEN ROUND(payment_type, 0)  = 2 THEN 'CASH'
        WHEN ROUND(payment_type, 0)  = 3 THEN 'NO-CHARGE'
        WHEN ROUND(payment_type, 0)  = 4 THEN 'DISPUTE'
        WHEN ROUND(payment_type, 0)  = 5 THEN 'UNKNOWN'
        WHEN ROUND(payment_type, 0)  = 6 THEN 'VOIDED TRIP'
        ELSE 'ERROR'
        END as payment_type,
    DATE_TRUNC('month', pickup_datetime) as month,
COUNT(*) as total_usage
FROM "nyc-trip-data_refined"."trip_data"
WHERE
    ROUND(payment_type, 0) = 1
   OR ROUND(payment_type, 0) = 2
  AND {{source}}
GROUP BY 1,2
ORDER BY 2,1 ASC
--
SELECT
    DATE_TRUNC('month', pickup_datetime) as month,
CASE
WHEN EXTRACT(hour from pickup_datetime) >= 6 AND EXTRACT(hour from pickup_datetime) <= 12 THEN 'MORNING'
WHEN EXTRACT(hour from pickup_datetime) >= 13 AND EXTRACT(hour from pickup_datetime) <= 18 THEN 'AFTERNOON'
WHEN EXTRACT(hour from pickup_datetime) >= 19 AND EXTRACT(hour from pickup_datetime) <= 23 THEN 'NIGHTIME'
WHEN EXTRACT(hour from pickup_datetime) >= 0 AND EXTRACT(hour from pickup_datetime) <= 5 THEN 'DAWN'
ELSE 'ERROR' END as hour_of_the_day,
COUNT(*) as total_trips
FROM "nyc-trip-data_refined"."trip_data"
WHERE
    {{source}}
GROUP BY 1,2
ORDER BY 1,2 ASC
--
SELECT
    ROUND(CAST(do_latitude AS double), 8) as lat,
    ROUND(CAST(do_longitude AS double), 8) as lon,
    COUNT(*) as total
FROM "nyc-trip-data_refined"."trip_data"
WHERE
    pu_longitude is not null
  AND pu_latitude is not null
  AND do_longitude is not null
  AND do_latitude is not null
  AND {{source}}
GROUP BY 1,2
ORDER BY 3

--

SELECT
    source,
    DATE_TRUNC('month', pickup_datetime) as month,
    AVG(total_amount) as avg_total_amount
FROM "nyc-trip-data_refined"."trip_data"
WHERE
    {{source}}
GROUP BY 1,2
ORDER BY 2,1 ASC