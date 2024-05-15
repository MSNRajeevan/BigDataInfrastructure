-- Query to filter weather data for locations with cold temperatures or rain
WITH cold_weather AS (
    SELECT
        location_name,
        uv_index_degree_celcius,
        weather_description
    FROM
        `scenic-style-420014.BigData767.weather1`
    WHERE
        -- Filter for temperatures below 21°C
        temperature_degree_celcius < 21
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
-- Main query to retrieve hotel data for flights landing in locations with cold weather
SELECT
    h.hotel_city,  -- City where the hotel is located
    lf.flight_name,  -- Name of the flight
    lf.airline_name,  -- Name of the airline
    h.hotel_title,  -- Title of the hotel
    h.rating,  -- Rating of the hotel
    h.price,  -- Price of the hotel
    h.provider,  -- Provider of the hotel booking
    h.external_url  -- External URL for booking the hotel
FROM
    landing_flights lf
JOIN
    cold_weather iw ON lf.arrival_airport LIKE CONCAT('%', iw.location_name, '%')  -- Joining with cold weather data
JOIN
    `scenic-style-420014.BigData767.hotels1` h ON lf.arrival_airport LIKE CONCAT('%', h.hotel_city, '%')  -- Joining with hotel data
ORDER BY
    lf.arrival_airport, h.rating DESC  -- Ordering by arrival airport and descending hotel rating
LIMIT 20; -- Limiting the output to 20 rows
