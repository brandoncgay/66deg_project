-- Sales Performance by Store with Window Functions
SELECT 
    s.city,
    s.branch,
    COUNT(f.sales_id) as transaction_count,
    ROUND(SUM(f.total), 2) as total_revenue,
    RANK() OVER (ORDER BY SUM(f.total) DESC) as revenue_rank,
    ROUND(SUM(SUM(f.total)) OVER (ORDER BY s.city, s.branch), 2) as running_total
FROM fact_sales f
JOIN dim_store s ON f.store_id = s.store_id  
GROUP BY s.city, s.branch
ORDER BY total_revenue DESC;