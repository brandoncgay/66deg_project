-- Product Performance Analysis by City with Window Functions
SELECT 
    p.product_line,
    s.city,
    s.branch,
    COUNT(f.sales_id) as transaction_count,
    ROUND(SUM(f.total), 2) as total_revenue,
    RANK() OVER (PARTITION BY s.city ORDER BY SUM(f.total) DESC) as city_rank
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_store s ON f.store_id = s.store_id
GROUP BY p.product_line, s.city, s.branch
ORDER BY s.city, total_revenue DESC;