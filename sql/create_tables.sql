-- Supermarket Sales Data Warehouse Schema
-- Generated on: 2025-08-31

-- DIM_PRODUCT
CREATE TABLE IF NOT EXISTS dim_product (
    product_id INTEGER PRIMARY KEY,
    product_line TEXT NOT NULL,
    unit_price REAL NOT NULL,
    cogs REAL NOT NULL,
    gross_margin_percentage REAL NOT NULL
);

-- DIM_STORE  
CREATE TABLE IF NOT EXISTS dim_store (
    store_id INTEGER PRIMARY KEY,
    branch TEXT NOT NULL,
    city TEXT NOT NULL
);

-- FACT_SALES
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_id INTEGER PRIMARY KEY,
    product_id INTEGER,
    store_id INTEGER,
    invoice_id TEXT,
    customer_type TEXT,
    gender TEXT,
    quantity INTEGER,
    tax_5 REAL,
    total REAL,
    date TEXT,
    time TEXT,
    payment TEXT,
    gross_income REAL,
    rating REAL,
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (store_id) REFERENCES dim_store(store_id)
);