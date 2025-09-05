-- Sales Performance by Store with Window Functions and Date Analysis
SELECT 
    s.city,
    s.branch,
    COUNT(f.sales_id) as transaction_count,
    ROUND(SUM(f.total), 2) as total_revenue,
    RANK() OVER (ORDER BY SUM(f.total) DESC) as revenue_rank,
    ROUND(SUM(SUM(f.total)) OVER (ORDER BY s.city, s.branch), 2) as running_total,
    
    -- Date-based analysis
    MIN(f.date) as first_sale_date,
    MAX(f.date) as last_sale_date,
    COUNT(DISTINCT f.date) as active_days,
    ROUND(SUM(f.total) / COUNT(DISTINCT f.date), 2) as avg_daily_revenue,
    
    -- Day of week analysis
    ROUND(AVG(CASE WHEN strftime('%w', f.date) IN ('0','6') 
        THEN f.total ELSE NULL END), 2) as weekend_avg_transaction,
    ROUND(AVG(CASE WHEN strftime('%w', f.date) NOT IN ('0','6') 
        THEN f.total ELSE NULL END), 2) as weekday_avg_transaction,
    
    -- Recent performance (last 7 days in dataset)
    SUM(CASE WHEN f.date >= (
        SELECT date(MAX(date), '-6 days') FROM fact_sales
    ) THEN f.total ELSE 0 END) as recent_7day_revenue
    
FROM fact_sales f
JOIN dim_store s ON f.store_id = s.store_id  
GROUP BY s.city, s.branch
ORDER BY total_revenue DESC;