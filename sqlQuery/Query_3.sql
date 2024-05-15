SELECT
    f.departure_airport AS Departure_Airport,
    AVG(f.arrival_delay_mins) AS Average_Arrival_Delay,
    f.arrival_airport AS Destination_Airport,
    COUNT(h.hotel_id) AS Number_of_Hotels,
    AVG(h.rating) AS Average_Hotel_Rating, h.price
FROM `scenic-style-420014.BigData767.flightsTable` AS f
    JOIN `scenic-style-420014.BigData767.hotelsTable` AS h
    ON TRIM(f.arrival_city) LIKE CONCAT('%', TRIM(h.hotel_city), '%')
    JOIN `scenic-style-420014.BigData767.weatherTable` AS w
    ON TRIM(f.arrival_city) LIKE CONCAT('%', TRIM(w.location_name), '%')
WHERE w.temperature_degree_celcius <=20 or w.feels_like_degree_celcius <=20
 -- Assuming below 20 degree celsius its cold
GROUP BY
    f.departure_airport, f.arrival_airport,h.price
    ORDER BY h.price DESC

