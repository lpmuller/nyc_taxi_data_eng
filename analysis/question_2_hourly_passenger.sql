-- Question 2:
-- What is the average passenger count by hour of day?

SELECT
    pickup_hour,
    avg_passenger_count,
    valid_passenger_records
FROM workspace.gold.yellow_taxi_hourly_passenger
ORDER BY pickup_hour;
