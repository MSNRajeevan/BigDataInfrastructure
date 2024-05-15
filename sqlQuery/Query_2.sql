WITH HotelRankings AS (
    SELECT
        f.arrival_airport AS Airport,
        h.hotel_title AS Hotel,
        h.rating AS Rating,
        h.hotel_city AS City,CASE
            WHEN is_day='TRUE' THEN 'Day'
            ELSE 'NIGHT'
        END AS Day_Category,
        ROW_NUMBER() OVER (
            PARTITION BY h.hotel_city,CASE
            WHEN is_day='TRUE' THEN 'Day'
            ELSE 'NIGHT'
        END
            ORDER BY h.rating DESC
        ) AS Rank
    FROM `scenic-style-420014.BigData767.flightsTable` AS f
    JOIN `scenic-style-420014.BigData767.hotelsTable` AS h
    ON TRIM(f.arrival_city) LIKE CONCAT('%', TRIM(h.hotel_city), '%')
    JOIN `scenic-style-420014.BigData767.weatherTable` AS w
    ON TRIM(f.arrival_city) LIKE CONCAT('%', TRIM(w.location_name), '%')

)

SELECT DISTINCT
    Airport,
    Hotel,
    Rating,
    City, Day_Category
FROM
    HotelRankings
WHERE
    Rank =1