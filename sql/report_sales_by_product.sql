-- Sales Performance by Product Line
SELECT 
    p.product_line,
    COUNT(f.sales_id) as transaction_count,
    ROUND(SUM(f.total), 2) as total_revenue,
    ROUND(AVG(f.total), 2) as avg_transaction_value,
    ROUND(AVG(f.rating), 2) as avg_rating
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.product_line
ORDER BY total_revenue DESC;