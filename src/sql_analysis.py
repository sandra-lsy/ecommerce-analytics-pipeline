import sqlite3
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SQLAnalyzer:
    """SQL Analysis for E-commerce Data"""
    
    def __init__(self, db_path='ecommerce.db'):
        self.db_path = db_path
    
    def run_query(self, query, query_name="Query"):
        """Run a SQL query and return results"""
        try:
            conn = sqlite3.connect(self.db_path)
            logger.info(f"🔍 Running {query_name}...")
            df = pd.read_sql_query(query, conn)
            conn.close()
            logger.info(f"✅ {query_name} completed - {len(df)} rows returned")
            return df
        except Exception as e:
            logger.error(f"❌ Error in {query_name}: {e}")
            return None
    
    def basic_stats(self):
        """Basic database statistics"""
        query = """
        SELECT 
            'Customers' as table_name, COUNT(*) as count FROM customers
        UNION ALL
        SELECT 
            'Products' as table_name, COUNT(*) as count FROM products
        UNION ALL
        SELECT 
            'Orders' as table_name, COUNT(*) as count FROM orders
        UNION ALL
        SELECT 
            'Completed Orders' as table_name, COUNT(*) as count 
        FROM orders WHERE status = 'Completed';
        """
        return self.run_query(query, "Basic Statistics")
    
    def monthly_revenue_analysis(self):
        """Monthly revenue trend analysis"""
        query = """
        SELECT 
            order_month,
            ROUND(SUM(total_amount), 2) as revenue,
            COUNT(*) as order_count,
            ROUND(AVG(total_amount), 2) as avg_order_value
        FROM orders 
        WHERE status = 'Completed'
        GROUP BY order_month
        ORDER BY order_month;
        """
        return self.run_query(query, "Monthly Revenue Analysis")
    
    def customer_segmentation(self):
        """Customer segmentation analysis"""
        query = """
        SELECT 
            customer_segment,
            COUNT(*) as customer_count,
            ROUND(AVG(CASE WHEN total_spent IS NULL THEN 0 ELSE total_spent END), 2) as avg_spent
        FROM customers c
        LEFT JOIN (
            SELECT 
                customer_id, 
                SUM(total_amount) as total_spent
            FROM orders 
            WHERE status = 'Completed'
            GROUP BY customer_id
        ) o ON c.customer_id = o.customer_id
        GROUP BY customer_segment
        ORDER BY avg_spent DESC;
        """
        return self.run_query(query, "Customer Segmentation")
    
    def geographic_analysis(self):
        """Revenue analysis by location"""
        query = """
        SELECT 
            c.location,
            COUNT(DISTINCT c.customer_id) as customer_count,
            COUNT(o.order_id) as total_orders,
            ROUND(COALESCE(SUM(o.total_amount), 0), 2) as total_revenue
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id AND o.status = 'Completed'
        GROUP BY c.location
        ORDER BY total_revenue DESC;
        """
        return self.run_query(query, "Geographic Analysis")
    
    def top_revenue_months(self):
        """Top performing months"""
        query = """
        SELECT 
            order_month,
            ROUND(SUM(total_amount), 2) as revenue,
            COUNT(*) as orders
        FROM orders 
        WHERE status = 'Completed'
        GROUP BY order_month
        ORDER BY revenue DESC
        LIMIT 5;
        """
        return self.run_query(query, "Top Revenue Months")
    
    def run_full_analysis(self):
        """Run complete SQL analysis"""
        print("\n" + "="*60)
        print("🚀 STARTING SQL ANALYSIS")
        print("="*60)
        
        # Run all analyses
        analyses = {
            'Database Overview': self.basic_stats(),
            'Monthly Revenue': self.monthly_revenue_analysis(),
            'Customer Segments': self.customer_segmentation(),
            'Geographic Performance': self.geographic_analysis(),
            'Top Revenue Months': self.top_revenue_months()
        }
        
        # Display results
        for name, df in analyses.items():
            if df is not None and len(df) > 0:
                print(f"\n📊 {name.upper()}:")
                print("-" * 40)
                print(df.to_string(index=False))
                print()
            else:
                print(f"\n❌ {name}: No data returned")
        
        print("="*60)
        print("✅ SQL ANALYSIS COMPLETE")
        print("="*60)
        
        return analyses

# Run the analysis
if __name__ == "__main__":
    analyzer = SQLAnalyzer()
    analyzer.run_full_analysis()