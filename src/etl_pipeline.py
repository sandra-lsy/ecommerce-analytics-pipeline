# src/etl_pipeline.py
import pandas as pd
import numpy as np
import sqlite3
import json
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ECommerceETL:
    """ETL Pipeline for E-commerce Data"""
    
    def __init__(self, db_path='ecommerce.db'):
        self.db_path = db_path
        self.customers = None
        self.products = None
        self.orders = None
    
    def extract_data(self):
        """Extract data from multiple sources"""
        logger.info("Starting data extraction...")
        
        try:
            # Extract from CSV
            self.customers = pd.read_csv('data/customers.csv')
            self.orders = pd.read_csv('data/orders.csv')
            
            # Extract from JSON
            with open('data/products.json', 'r') as f:
                products_data = json.load(f)
            self.products = pd.DataFrame(products_data)
            
            logger.info(f"✅ Extracted {len(self.customers)} customers")
            logger.info(f"✅ Extracted {len(self.products)} products")
            logger.info(f"✅ Extracted {len(self.orders)} orders")
            
        except Exception as e:
            logger.error(f"❌ Error during extraction: {e}")
            raise
    
    def transform_data(self):
        """Transform and clean the data"""
        logger.info("Starting data transformation...")
        
        try:
            # Transform customers data
            self.customers['signup_date'] = pd.to_datetime(self.customers['signup_date'])
            self.customers['days_since_signup'] = (datetime.now() - self.customers['signup_date']).dt.days
            
            # Transform orders data
            self.orders['order_date'] = pd.to_datetime(self.orders['order_date'])
            self.orders['order_month'] = self.orders['order_date'].dt.to_period('M').astype(str)
            self.orders['order_year'] = self.orders['order_date'].dt.year
            
            # Add derived columns to products data
            self.products['profit_margin'] = ((self.products['price'] - self.products['cost']) / self.products['price'] * 100).round(2)
            
            # Data quality checks
            self._validate_data()
            
            logger.info("✅ Data transformation completed")
            
        except Exception as e:
            logger.error(f"❌ Error during transformation: {e}")
            raise
    
    def _validate_data(self):
        """Validate data quality"""
        logger.info("Performing data quality checks...")
        
        # Check for missing values in all datasets
        customers_missing = self.customers.isnull().sum().sum()
        products_missing = self.products.isnull().sum().sum()
        orders_missing = self.orders.isnull().sum().sum()
        
        if customers_missing > 0:
            logger.warning(f"⚠️ {customers_missing} missing values in customers data")
        
        if products_missing > 0:
            logger.warning(f"⚠️ {products_missing} missing values in products data")
            
        if orders_missing > 0:
            logger.warning(f"⚠️ {orders_missing} missing values in orders data")
        
        # Check for duplicates
        customer_dupes = self.customers.duplicated().sum()
        if customer_dupes > 0:
            logger.warning(f"⚠️ {customer_dupes} duplicate customers found")
        
        logger.info("✅ Data quality checks completed")
    
    def load_data(self):
        """Load data into SQLite database"""
        logger.info("Loading data into database...")
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Load tables into database
            self.customers.to_sql('customers', conn, if_exists='replace', index=False)
            self.products.to_sql('products', conn, if_exists='replace', index=False)
            self.orders.to_sql('orders', conn, if_exists='replace', index=False)
            
            # Create indexes for performance
            cursor = conn.cursor()
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_customer_id ON customers(customer_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_product_id ON products(product_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_order_date ON orders(order_date)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_order_customer ON orders(customer_id)")
            
            conn.commit()
            conn.close()
            
            logger.info("✅ Data loaded successfully into database")
            
        except Exception as e:
            logger.error(f"❌ Error during loading: {e}")
            raise
    
    def run_pipeline(self):
        """Run the complete ETL pipeline"""
        logger.info("🚀 Starting ETL Pipeline...")
        start_time = datetime.now()
        
        # Run ETL pipeline
        self.extract_data()
        self.transform_data()
        self.load_data()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info(f"✅ ETL Pipeline completed in {duration:.2f} seconds")
        
        # Print summary
        self._print_summary()
    
    def _print_summary(self):
        """Print pipeline summary"""
        print("\n" + "="*50)
        print("📊 ETL PIPELINE SUMMARY")
        print("="*50)
        print(f"- Customers: {len(self.customers):,}")
        print(f"- Products: {len(self.products):,}")
        print(f"- Orders: {len(self.orders):,}")
        print(f"- Total Revenue: £{self.orders['total_amount'].sum():,.2f}")
        print(f"- Date Range: {self.orders['order_date'].min().date()} to {self.orders['order_date'].max().date()}")
        print("="*50)

# Run the pipeline
if __name__ == "__main__":
    etl = ECommerceETL()
    etl.run_pipeline()