-- business_queries.sql
-- ADVANCED SQL SHOWCASE
-- 
-- This file demonstrates complex SQL capabilities including:
-- - CTEs (Common Table Expressions)
-- - Window functions (LAG, FIRST_VALUE)
-- - Advanced analytics (cohort analysis, growth rates)
-- - Customer segmentation logic
-- 
-- Note: These queries showcase SQL skills but may require 
-- additional data model complexity (e.g., order_items table)
-- For working queries used in the pipeline, see sql_analysis.py

-- 1. Monthly Revenue Trend with Growth Rate
WITH monthly_revenue AS (
    SELECT 
        order_month,
        SUM(total_amount) as revenue,
        COUNT(*) as order_count,
        AVG(total_amount) as avg_order_value
    FROM orders 
    WHERE status = 'Completed'
    GROUP BY order_month
    ORDER BY order_month
),
revenue_with_growth AS (
    SELECT 
        *,
        LAG(revenue) OVER (ORDER BY order_month) as prev_month_revenue,
        ROUND(
            ((revenue - LAG(revenue) OVER (ORDER BY order_month)) / 
             LAG(revenue) OVER (ORDER BY order_month)) * 100, 2
        ) as growth_rate
    FROM monthly_revenue
)
SELECT * FROM revenue_with_growth;

-- 2. Customer Lifetime Value & Segmentation
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        c.name,
        c.location,
        c.customer_segment,
        c.signup_date,
        COUNT(o.order_id) as total_orders,
        SUM(o.total_amount) as lifetime_value,
        AVG(o.total_amount) as avg_order_value,
        MIN(o.order_date) as first_order_date,
        MAX(o.order_date) as last_order_date,
        JULIANDAY('now') - JULIANDAY(MAX(o.order_date)) as days_since_last_order
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id AND o.status = 'Completed'
    GROUP BY c.customer_id, c.name, c.location, c.customer_segment, c.signup_date
),
customer_segments AS (
    SELECT 
        *,
        CASE 
            WHEN lifetime_value >= 1000 AND days_since_last_order <= 30 THEN 'VIP Active'
            WHEN lifetime_value >= 500 AND days_since_last_order <= 60 THEN 'High Value'
            WHEN lifetime_value >= 100 AND days_since_last_order <= 90 THEN 'Regular'
            WHEN days_since_last_order > 180 THEN 'At Risk'
            ELSE 'New/Low Value'
        END as calculated_segment
    FROM customer_metrics
)
SELECT * FROM customer_segments
ORDER BY lifetime_value DESC;

-- 3. Product Performance Analysis
WITH product_performance AS (
    SELECT 
        p.product_id,
        p.name,
        p.category,
        p.price,
        p.profit_margin,
        COUNT(o.order_id) as times_ordered,
        SUM(o.total_amount) as total_revenue,
        AVG(o.total_amount) as avg_order_value
    FROM products p
    LEFT JOIN orders o ON p.product_id IN (
        -- This is simplified - in real scenario you'd have order_items table
        SELECT CAST(SUBSTR(p.name, -2) AS INTEGER) 
        FROM products p2 
        WHERE p2.product_id = p.product_id
    )
    GROUP BY p.product_id, p.name, p.category, p.price, p.profit_margin
),
category_stats AS (
    SELECT 
        category,
        COUNT(*) as product_count,
        AVG(profit_margin) as avg_margin,
        SUM(total_revenue) as category_revenue,
        AVG(times_ordered) as avg_popularity
    FROM product_performance
    GROUP BY category
)
SELECT * FROM category_stats
ORDER BY category_revenue DESC;

-- 4. Geographic Analysis
SELECT 
    c.location,
    COUNT(DISTINCT c.customer_id) as customer_count,
    COUNT(o.order_id) as total_orders,
    SUM(o.total_amount) as total_revenue,
    AVG(o.total_amount) as avg_order_value,
    ROUND(SUM(o.total_amount) / COUNT(DISTINCT c.customer_id), 2) as revenue_per_customer
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id AND o.status = 'Completed'
GROUP BY c.location
ORDER BY total_revenue DESC;

-- 5. Advanced: Cohort Analysis (Monthly)
WITH first_purchase AS (
    SELECT 
        customer_id,
        DATE(MIN(order_date), 'start of month') as cohort_month
    FROM orders
    WHERE status = 'Completed'
    GROUP BY customer_id
),
customer_activity AS (
    SELECT 
        fp.cohort_month,
        DATE(o.order_date, 'start of month') as activity_month,
        COUNT(DISTINCT fp.customer_id) as customers
    FROM first_purchase fp
    LEFT JOIN orders o ON fp.customer_id = o.customer_id AND o.status = 'Completed'
    GROUP BY fp.cohort_month, DATE(o.order_date, 'start of month')
),
cohort_data AS (
    SELECT 
        cohort_month,
        activity_month,
        customers,
        ROUND(
            (JULIANDAY(activity_month) - JULIANDAY(cohort_month)) / 30.44, 0
        ) as period_number
    FROM customer_activity
)
SELECT 
    cohort_month,
    period_number,
    customers,
    FIRST_VALUE(customers) OVER (
        PARTITION BY cohort_month 
        ORDER BY period_number
    ) as cohort_size,
    ROUND(
        customers * 100.0 / FIRST_VALUE(customers) OVER (
            PARTITION BY cohort_month 
            ORDER BY period_number
        ), 2
    ) as retention_rate
FROM cohort_data
ORDER BY cohort_month, period_number;