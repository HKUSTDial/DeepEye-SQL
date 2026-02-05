WITH abakan_airports AS (
    SELECT airport_code FROM airports_data 
    WHERE json_extract(city, '$.en') = 'Abakan' OR json_extract(city, '$.ru') = 'Абакан'
),
flight_coords AS (
    SELECT 
        f.flight_id,
        CAST(substr(dep.coordinates, 2, instr(dep.coordinates, ',') - 2) AS REAL) AS dep_lon,
        CAST(substr(dep.coordinates, instr(dep.coordinates, ',') + 1, length(dep.coordinates) - instr(dep.coordinates, ',') - 1) AS REAL) AS dep_lat,
        CAST(substr(arr.coordinates, 2, instr(arr.coordinates, ',') - 2) AS REAL) AS arr_lon,
        CAST(substr(arr.coordinates, instr(arr.coordinates, ',') + 1, length(arr.coordinates) - instr(arr.coordinates, ',') - 1) AS REAL) AS arr_lat
    FROM flights f
    JOIN airports_data dep ON f.departure_airport = dep.airport_code
    JOIN airports_data arr ON f.arrival_airport = arr.airport_code
    WHERE f.departure_airport IN (SELECT airport_code FROM abakan_airports)
       OR f.arrival_airport IN (SELECT airport_code FROM abakan_airports)
),
distances AS (
    SELECT 
        2 * 6371 * asin(sqrt(
            sin((arr_lat - dep_lat) * 3.141592653589793 / 360.0) * sin((arr_lat - dep_lat) * 3.141592653589793 / 360.0) +
            cos(dep_lat * 3.141592653589793 / 180.0) * cos(arr_lat * 3.141592653589793 / 180.0) *
            sin((arr_lon - dep_lon) * 3.141592653589793 / 360.0) * sin((arr_lon - dep_lon) * 3.141592653589793 / 360.0)
        )) AS distance_km
    FROM flight_coords
)
SELECT MAX(distance_km) AS longest_route_distance_km
FROM distances