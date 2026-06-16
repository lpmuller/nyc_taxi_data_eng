-- Question 1:
-- What is the average total amount received in a month considering all yellow taxis?

SELECT
    pickup_month,
    avg_total_amount,
    total_trips
FROM workspace.gold.yellow_taxi_monthly_amount
ORDER BY pickup_month;
