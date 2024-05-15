-- Query to find the top 10 most frequent flight routes
WITH frequent_flights AS (
    SELECT
        departure_city,
        arrival_city,
        COUNT(*) AS flight_count
    FROM
        `scenic-style-420014.BigData767.flightsTable`
    GROUP BY
        departure_city,
        arrival_city
    ORDER BY
        flight_count DESC
    LIMIT 10
),
-- Query to retrieve all hotels with their titles, ratings, and prices
all_hotels AS (
    SELECT
        hotel_city,
        hotel_title,
        rating,
        CAST(REPLACE(REPLACE(price, '$', ''), ',', '') AS FLOAT64) AS price_num,  -- Cleaning and converting price to FLOAT64
        price
    FROM
        `scenic-style-420014.BigData767.hotelsTable`
    WHERE
        price IS NOT NULL AND price != ''  -- Filtering out NULL or empty prices
)

-- Main query to join frequent flight routes with hotels based on arrival cities
SELECT
    ff.departure_city,  -- Departure city of the frequent flight route
    ff.arrival_city,  -- Arrival city of the frequent flight route
    ff.flight_count,  -- Number of flights for the route
    ah.hotel_title,  -- Title of the hotel in the arrival city
    ah.rating, -- Rating of the hotel
    ah.price  -- Original price string
FROM
    frequent_flights ff
JOIN
    all_hotels ah ON ff.arrival_city LIKE CONCAT('%', ah.hotel_city, '%')  -- Joining with hotels based on arrival city
ORDER BY
    ah.price_num ASC;  -- Ordering by the numerical price