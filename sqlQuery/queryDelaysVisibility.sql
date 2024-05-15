--- Promoting hotels to travelers with potential prolonged delay at departure airport

WITH weather_conditions AS (
    SELECT 
        location_name, 
        visibility_m
    FROM 
        `scenic-style-420014.BigData767.weatherTable`
    WHERE
        -- in this demo, we changed the visibility threshold to 11 allow some results to be returned  
        visibility_m < 11
),
delayed_flights AS (
    SELECT 
        departure_city,
        departure_delay_mins,
        flight_name,
        airline_name
    FROM 
        `scenic-style-420014.BigData767.flightsTable`
    WHERE
        -- in this demo, we changed the delay mins to 6 to allow some results to be returned 
        departure_delay_mins > 6
)
SELECT
    h.hotel_city,
    df.flight_name,
    df.airline_name,
    h.hotel_title,
    h.rating,
    h.price,
    h.provider,
    h.external_url
FROM 
    delayed_flights df
JOIN 
    weather_conditions wc ON df.departure_city LIKE CONCAT('%', wc.location_name, '%')
JOIN 
    `scenic-style-420014.BigData767.hotelsTable` h ON df.departure_city LIKE CONCAT('%', h.hotel_city, '%')
ORDER BY 
    df.departure_city, h.rating DESC
-- in this demo, we limit the number of hotels returned to 10
LIMIT 10;