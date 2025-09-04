#!/usr/bin/env python3
"""
Simplified Supermarket Sales Data Pipeline
Single file that handles extraction, transformation, loading, and reporting
"""

import os
import pandas as pd
import sqlite3
from pathlib import Path
import kaggle

def extract_data():
    """Extract data from Kaggle"""
    print("🔄 Extracting data from Kaggle...")
    
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    # Check for Kaggle credentials
    if not os.path.exists(os.path.expanduser("~/.kaggle/kaggle.json")):
        print("❌ Please place kaggle.json in ~/.kaggle/ directory")
        return None
    
    try:
        # Download dataset
        kaggle.api.dataset_download_files(
            "lovishbansal123/sales-of-a-supermarket",
            path=data_dir,
            unzip=True
        )
        
        # Load CSV
        csv_files = list(data_dir.glob("*.csv"))
        if csv_files:
            df = pd.read_csv(csv_files[0])
            print(f"✅ Extracted {len(df)} rows with {len(df.columns)} columns")
            return df
        else:
            print("❌ No CSV files found")
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def transform_data(df):
    """Transform transaction data into dimensional model"""
    print("🔄 Transforming data...")
    
    # Clean column names - handle the actual column names
    df.columns = df.columns.str.strip().str.replace(' ', '_').str.lower()
    print(f"📋 Available columns: {list(df.columns)}")
    print(f"📊 Data shape: {df.shape}")
    
    # 1. Product Dimension
    product_cols = ['product_line', 'unit_price', 'cogs', 'gross_margin_percentage']
    dim_product = df[product_cols].drop_duplicates().reset_index(drop=True)
    dim_product['product_id'] = range(1, len(dim_product) + 1)
    dim_product = dim_product[['product_id'] + product_cols]
    
    # 2. Store Dimension
    store_cols = ['branch', 'city']
    dim_store = df[store_cols].drop_duplicates().reset_index(drop=True)
    dim_store['store_id'] = range(1, len(dim_store) + 1)
    dim_store = dim_store[['store_id'] + store_cols]
    
    # 3. Sales Fact Table
    # Merge to get foreign keys
    fact_sales = df.merge(
        dim_product[['product_id', 'product_line', 'unit_price', 'cogs', 'gross_margin_percentage']], 
        on=['product_line', 'unit_price', 'cogs', 'gross_margin_percentage'],
        how='left'
    ).merge(
        dim_store[['store_id', 'branch', 'city']], 
        on=['branch', 'city'],
        how='left'
    )
    
    # Select fact columns (using actual column names and remove duplicates)
    fact_cols = ['invoice_id', 'customer_type', 'gender', 'quantity', 'tax_5', 'total', 
                'date', 'time', 'payment', 'gross_income', 'rating']
    
    # Fix column names to match SQL schema
    fact_sales = fact_sales.rename(columns={'tax_5%': 'tax_5'})
    
    # Ensure the rename worked
    print(f"📋 Fact columns after rename: {list(fact_sales.columns)}")
    
    # Filter to only columns that exist and add the foreign keys
    available_fact_cols = ['product_id', 'store_id'] + [col for col in fact_cols if col in fact_sales.columns]
    
    # Remove any duplicate columns
    available_fact_cols = list(dict.fromkeys(available_fact_cols))
    
    fact_sales = fact_sales[available_fact_cols].fillna(0)
    fact_sales['sales_id'] = range(1, len(fact_sales) + 1)
    fact_sales = fact_sales[['sales_id'] + available_fact_cols]
    
    print(f"✅ Created: {len(dim_product)} products, {len(dim_store)} stores, {len(fact_sales)} sales transactions")
    
    return dim_product, dim_store, fact_sales

def create_database_schema(db_path):
    """Create database tables using SQL schema file"""
    # Try multiple possible locations for the schema file
    possible_paths = [
        Path(__file__).parent / "sql/create_tables.sql" if __file__ else None,
        Path.cwd() / "sql/create_tables.sql",
        Path.cwd().parent / "sql/create_tables.sql",
        Path("../sql/create_tables.sql"),
        Path("sql/create_tables.sql")
    ]
    
    schema_file = None
    for path in possible_paths:
        if path and path.exists():
            schema_file = path
            break
    
    with sqlite3.connect(db_path) as conn:
        # Drop existing tables to ensure clean schema
        conn.execute("DROP TABLE IF EXISTS fact_sales")
        conn.execute("DROP TABLE IF EXISTS dim_product") 
        conn.execute("DROP TABLE IF EXISTS dim_store")
        
        # Execute schema from SQL file
        if schema_file and schema_file.exists():
            with open(schema_file, 'r') as f:
                schema_sql = f.read()
            conn.executescript(schema_sql)
            print(f"✅ Database schema created from SQL file: {schema_file}")
        else:
            print("❌ Schema file not found, creating tables with pandas")

def load_data(dim_product, dim_store, fact_sales):
    """Load data into SQLite database"""
    print("🔄 Loading data into database...")
    
    db_path = "data/supermarket_sales.db"
    Path(db_path).parent.mkdir(exist_ok=True)
    
    # Create schema first
    create_database_schema(db_path)
    
    with sqlite3.connect(db_path) as conn:
        # Load dimension tables first (due to foreign key constraints)
        dim_product.to_sql('dim_product', conn, if_exists='append', index=False)
        dim_store.to_sql('dim_store', conn, if_exists='append', index=False)
        fact_sales.to_sql('fact_sales', conn, if_exists='append', index=False)
        
        print(f"✅ Data loaded successfully")
        return db_path

def execute_sql_file(conn, sql_file_path):
    """Execute SQL from file and return DataFrame"""
    with open(sql_file_path, 'r') as f:
        query = f.read()
    return pd.read_sql(query, conn)

def generate_reports(db_path):
    """Generate automated reports from SQL files"""
    print("🔄 Generating reports...")
    
    with sqlite3.connect(db_path) as conn:
        
        # First, check what columns are actually available
        columns = pd.read_sql("PRAGMA table_info(fact_sales)", conn)
        print(f"📋 Fact table columns: {list(columns['name'])}")
        
        # Execute SQL report files - handle path from different working directories
        possible_sql_dirs = [
            Path(__file__).parent / "sql" if __file__ else None,
            Path.cwd() / "sql",
            Path.cwd().parent / "sql",
            Path("../sql"),
            Path("sql")
        ]
        
        sql_dir = None
        for path in possible_sql_dirs:
            if path and path.exists():
                sql_dir = path
                break
        
        if not sql_dir:
            print("❌ SQL directory not found")
            return
            
        report_files = {
            "Sales by Product Line": sql_dir / "report_sales_by_product.sql",
            "Sales by Store": sql_dir / "report_sales_by_store.sql"
        }
        
        reports = {}
        for report_name, sql_file in report_files.items():
            try:
                if sql_file.exists():
                    reports[report_name] = execute_sql_file(conn, sql_file)
                    print(f"\n📊 {report_name.upper()}")
                    print(reports[report_name])
                else:
                    print(f"❌ SQL file not found: {sql_file}")
            except Exception as e:
                print(f"❌ Error executing {report_name}: {e}")
        
        # Save reports
        report_dir = Path("data/reports")
        report_dir.mkdir(exist_ok=True)
        
        for report_name, df in reports.items():
            filename = report_name.lower().replace(' ', '_') + '.csv'
            df.to_csv(report_dir / filename, index=False)
        
        print(f"✅ Reports saved to {report_dir}/")

def run_simple_pipeline():
    """Run the complete simplified pipeline"""
    print("🚀 Starting Simplified Data Pipeline")
    print("=" * 50)
    
    # Extract
    df = extract_data()
    if df is None:
        return
    
    # Transform  
    dim_product, dim_store, fact_sales = transform_data(df)
    if dim_product is None:
        return
        
    # Load
    db_path = load_data(dim_product, dim_store, fact_sales)
    
    # Report
    generate_reports(db_path)
    
    print("\n✅ Pipeline completed successfully!")

if __name__ == "__main__":
    run_simple_pipeline()