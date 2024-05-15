-- Promoting high quality hotels to travelers at locations where they might prefer to spend more time indoor (with high UV index or rainy)

WITH indoor_weather AS (
    SELECT
        location_name,
        uv_index_degree_celcius,
        weather_description
    FROM
        `scenic-style-420014.BigData767.weatherTable`
    WHERE
        -- in this demo, UV index threshold is changed to 0 to allow query to return results
        uv_index_degree_celcius > 0
        OR weather_description LIKE '%Rain%'
),
landing_flights AS (
    SELECT
        arrival_city,
        flight_name,
        airline_name
    FROM
        `scenic-style-420014.BigData767.flightsTable`
)
SELECT
    h.hotel_city,
    lf.flight_name,
    lf.airline_name,
    h.hotel_title,
    h.rating,
    h.price,
    h.provider,
    h.external_url
FROM
    landing_flights lf
JOIN
    indoor_weather iw ON lf.arrival_city LIKE CONCAT('%', iw.location_name, '%')
JOIN
    `scenic-style-420014.BigData767.hotelsTable` h ON lf.arrival_city LIKE CONCAT('%', h.hotel_city, '%')
ORDER BY
    lf.arrival_city, h.rating DESC
-- in this demo, we limit the number of hotels returned to 10
LIMIT 10;