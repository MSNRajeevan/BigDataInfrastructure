WITH PressureImpact AS (
    SELECT
        f.departure_airport AS Departure_Airport,
        AVG(f.arrival_delay_mins) AS Average_Arrival_Delay,
        f.arrival_airport AS Destination_Airport,
        COUNT(h.hotel_id) AS Number_of_Hotels,
       AVG(h.rating) AS Average_Hotel_Rating,
       MIN(w.pressure_Pa) AS Min_Pressure
    FROM `scenic-style-420014.BigData767.flightsTable` AS f
    JOIN `scenic-style-420014.BigData767.hotelsTable` AS h
    ON TRIM(f.arrival_city) LIKE CONCAT('%', TRIM(h.hotel_city), '%')
    JOIN `scenic-style-420014.BigData767.weatherTable` AS w
    ON TRIM(f.arrival_city) LIKE CONCAT('%', TRIM(w.location_name), '%')
    GROUP BY
        f.departure_airport, f.arrival_airport
)
SELECT
    Departure_Airport,
    Average_Arrival_Delay,
    Destination_Airport,
    Number_of_Hotels,
    Average_Hotel_Rating
FROM
    PressureImpact
WHERE Min_Pressure < 101325
