-- Product Performance Analysis by City with Window Functions and Date Analysis
SELECT 
    p.product_line,
    s.city,
    s.branch,
    COUNT(f.sales_id) as transaction_count,
    ROUND(SUM(f.total), 2) as total_revenue,
    RANK() OVER (PARTITION BY s.city ORDER BY SUM(f.total) DESC) as city_rank,
    
    -- Date-based analysis
    MIN(f.date) as first_sale_date,
    MAX(f.date) as last_sale_date,
    COUNT(DISTINCT f.date) as active_days,
    ROUND(SUM(f.total) / COUNT(DISTINCT f.date), 2) as avg_daily_revenue,
    
    -- Monthly trend analysis
    COUNT(CASE WHEN strftime('%m', f.date) = '01' THEN 1 END) as jan_transactions,
    COUNT(CASE WHEN strftime('%m', f.date) = '02' THEN 1 END) as feb_transactions,
    COUNT(CASE WHEN strftime('%m', f.date) = '03' THEN 1 END) as mar_transactions,
    
    -- Peak day analysis
    strftime('%w', f.date) as best_day_of_week,
    MAX(CASE WHEN strftime('%w', f.date) = '0' THEN f.total ELSE 0 END) as sunday_peak,
    MAX(CASE WHEN strftime('%w', f.date) = '6' THEN f.total ELSE 0 END) as saturday_peak
    
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_store s ON f.store_id = s.store_id
GROUP BY p.product_line, s.city, s.branch
HAVING COUNT(f.sales_id) >= 3  -- Filter for statistical significance
ORDER BY s.city, total_revenue DESC;