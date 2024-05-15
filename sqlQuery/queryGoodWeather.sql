-- Promoting travel package to sunny cities

WITH sunny_weather AS (
    SELECT
        location_name,
        weather_description
    FROM
        `scenic-style-420014.BigData767.weatherTable`
    WHERE
        -- Original query for good weather
        -- weather_description LIKE '%Clear%' OR
        -- weather_description LIKE '%Sunny%'

        --in this demo, changed the weather criteria to Partly cloudy to allow results to be returned
        weather_description LIKE '%Partly cloudy%'
),
flights_to_sunny_cities AS (
    SELECT
        arrival_city,
        flight_name,
        airline_name
    FROM
        `scenic-style-420014.BigData767.flightsTable`
)
SELECT
    h.hotel_city,
    fsc.flight_name,
    fsc.airline_name,
    h.hotel_title,
    h.rating,
    h.price,
    h.provider,
    h.external_url
FROM
    flights_to_sunny_cities fsc
JOIN
    sunny_weather sw ON fsc.arrival_city LIKE CONCAT('%', sw.location_name, '%')
JOIN
    `scenic-style-420014.BigData767.hotelsTable` h ON fsc.arrival_city LIKE CONCAT('%', h.hotel_city, '%')
ORDER BY
    fsc.arrival_city, h.rating DESC
-- in this demo, we limit the number of hotels returned to 10
LIMIT 10;
