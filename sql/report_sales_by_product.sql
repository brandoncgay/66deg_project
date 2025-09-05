-- Sales Performance by Product Line with Date Analysis
SELECT 
    p.product_line,
    COUNT(f.sales_id) as transaction_count,
    ROUND(SUM(f.total), 2) as total_revenue,
    ROUND(AVG(f.total), 2) as avg_transaction_value,
    ROUND(AVG(f.rating), 2) as avg_rating,
    
    -- Date-based analysis
    MIN(f.date) as first_sale_date,
    MAX(f.date) as last_sale_date,
    COUNT(DISTINCT f.date) as active_days,
    ROUND(SUM(f.total) / COUNT(DISTINCT f.date), 2) as avg_daily_revenue,
    
    -- Time pattern analysis
    ROUND(AVG(CASE WHEN strftime('%H', f.time) BETWEEN '09' AND '12' 
        THEN f.total ELSE NULL END), 2) as morning_avg_transaction,
    ROUND(AVG(CASE WHEN strftime('%H', f.time) BETWEEN '13' AND '17' 
        THEN f.total ELSE NULL END), 2) as afternoon_avg_transaction,
    ROUND(AVG(CASE WHEN strftime('%H', f.time) BETWEEN '18' AND '21' 
        THEN f.total ELSE NULL END), 2) as evening_avg_transaction
        
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.product_line
ORDER BY total_revenue DESC;