-- Query to find the top 10 arrival airports with the most frequent flights
WITH frequent_flights AS (
    SELECT
        arrival_airport,
        COUNT(*) AS flight_count
    FROM
        `scenic-style-420014.BigData767.flights1`
    GROUP BY
        arrival_airport
    ORDER BY
        flight_count DESC
    LIMIT 10
),
-- Query to filter indoor weather data for locations with UV index greater than 0 or containing 'Rain' in the description
weather AS (
    SELECT
        location_name,
        uv_index_degree_celcius,
        weather_description
    FROM
        `scenic-style-420014.BigData767.weather1`
    WHERE
        uv_index_degree_celcius > 0
        OR weather_description LIKE '%Rain%'
),
-- Query to fetch landing flights data
landing_flights AS (
    SELECT
        arrival_airport,
        flight_name,
        airline_name
    FROM
        `scenic-style-420014.BigData767.flights1`
)
-- Main query to join the pre-calculated data and perform aggregations
SELECT
    lf.arrival_airport,  -- Arrival airport code
    iw.weather_description,  -- Weather description
    AVG(h.rating) AS average_rating,  -- Average hotel rating
    AVG(h.price) AS average_price,  -- Average hotel price
    MIN(h.rating) AS min_rating,  -- Minimum hotel rating
    MAX(h.rating) AS max_rating,  -- Maximum hotel rating
    COUNT(*) AS number_of_hotels,  -- Number of hotels in the area
    SUM(ff.flight_count) AS total_flights  -- Total number of flights
FROM
    landing_flights lf
JOIN
    frequent_flights ff ON lf.arrival_airport = ff.arrival_airport
JOIN
    weather iw ON lf.arrival_airport LIKE CONCAT('%', iw.location_name, '%')
JOIN
    `scenic-style-420014.BigData767.hotels1` h ON lf.arrival_airport LIKE CONCAT('%', h.hotel_city, '%')
GROUP BY
    lf.arrival_airport, iw.weather_description   -- Grouping by airport and weather description
ORDER BY
    total_flights DESC, average_rating DESC  -- Ordering by total flights and average rating
LIMIT 10;   -- Limiting the output to 10 rows
