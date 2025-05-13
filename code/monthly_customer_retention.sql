WITH raw_data_cast AS (
    -- Cast all columns from the raw_orders source to their appropriate data types
    SELECT
        CAST(customer_id AS INTEGER) AS customer_id,
        CAST(order_date AS TIMESTAMP) AS order_date,
        CAST(order_id AS INTEGER) AS order_id
    FROM {{ source('raw_orders', 'raw_orders') }}
),

customer_orders AS (
    -- Aggregate customer orders by month
    SELECT
        customer_id,
        DATE_TRUNC('month', order_date) AS order_month,
        COUNT(order_id) AS total_orders
    FROM raw_data_cast
    GROUP BY customer_id, DATE_TRUNC('month', order_date)
),

monthly_retention AS (
    -- Identify customers who purchased in consecutive months
    SELECT
        curr.customer_id,
        curr.order_month AS current_month,
        prev.order_month AS previous_month,
        CASE
            WHEN prev.customer_id IS NOT NULL THEN TRUE
            ELSE FALSE
        AS retained
    FROM customer_orders curr
    LEFT JOIN customer_orders prev
        ON curr.customer_id = prev.customer_id
        AND curr.order_month = prev.order_month + INTERVAL '1 month'
)

SELECT
    CAST(current_month AS DATE) AS current_month,
    CAST(COUNT(DISTINCT customer_id) AS INTEGER) AS total_customers,
    CAST(SUM(CASE WHEN retained THEN 1 ELSE 0 END) AS INTEGER) AS retained_customers,
    CAST(ROUND(SUM(CASE WHEN retained THEN 1 ELSE 0 END)::NUMERIC / COUNT(DISTINCT customer_id), 2) AS NUMERIC) AS retention_rate
FROM monthly_retention
GROUP BY current_month
ORDER BY current_month;
