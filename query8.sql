-- Query to filter departure weather data for locations with UV index greater than 0 or containing 'Rain' in the description
WITH departure_weather AS (
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
-- Query to fetch departure flights data along with arrival delay information
departure_flights AS (
departure_flights AS (
    SELECT
        departure_airport,
        arrival_delay_mins
    FROM
        `scenic-style-420014.BigData767.flights1`
),
-- Query to fetch departure hotels data along with their ratings
departure_hotels AS (
    SELECT
        h.hotel_city,
        h.rating
    FROM
        `scenic-style-420014.BigData767.hotels1` h
)
-- Main query to join the pre-calculated data and perform aggregations
SELECT
    df.departure_airport,
    dw.weather_description,
    AVG(df.arrival_delay_mins) AS average_delay,  -- Average arrival delay for flights departing from this city
    AVG(dh.rating) AS average_rating  -- Average hotel rating in the departure city
FROM
    departure_flights df
JOIN
    departure_weather dw ON df.departure_airport LIKE CONCAT('%', dw.location_name, '%')
LEFT JOIN
    departure_hotels dh ON df.departure_airport LIKE CONCAT('%', dh.hotel_city, '%')
GROUP BY
    df.departure_airport, dw.weather_description  -- Grouping by departure city and weather description
ORDER BY 
    average_delay DESC, average_rating DESC;  -- Ordering by average delay and average rating
