import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sqlite3
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Set style for professional-looking plots
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

class ECommerceVisualiser:
    """Create professional visualisations for e-commerce analytics"""
    
    def __init__(self, db_path='ecommerce.db'):
        self.db_path = db_path
        self.fig_size = (15, 12)
        
    def load_data(self):
        """Load data from database"""
        conn = sqlite3.connect(self.db_path)
        
        # Load main datasets
        self.customers = pd.read_sql_query("SELECT * FROM customers", conn)
        self.orders = pd.read_sql_query("SELECT * FROM orders WHERE status = 'Completed'", conn)
        self.products = pd.read_sql_query("SELECT * FROM products", conn)
        
        # Convert date columns
        self.orders['order_date'] = pd.to_datetime(self.orders['order_date'])
        self.customers['signup_date'] = pd.to_datetime(self.customers['signup_date'])
        
        conn.close()
        print("✅ Data loaded successfully")
    
    def create_revenue_dashboard(self):
        """Create comprehensive revenue analysis dashboard"""
        fig, axes = plt.subplots(2, 2, figsize=self.fig_size)
        fig.suptitle('📊 E-Commerce Revenue Analytics Dashboard', fontsize=16, fontweight='bold')
        
        # 1. Monthly Revenue Trend
        monthly_revenue = self.orders.groupby('order_month')['total_amount'].agg(['sum', 'count', 'mean']).reset_index()
        monthly_revenue.columns = ['month', 'revenue', 'orders', 'avg_order_value']
        monthly_revenue = monthly_revenue.sort_values('month')
        
        axes[0,0].plot(monthly_revenue['month'], monthly_revenue['revenue'], 
                      marker='o', linewidth=2, markersize=6, color='#2E86AB')
        axes[0,0].set_title('💰 Monthly Revenue Trend', fontweight='bold')
        axes[0,0].set_xlabel('Month')
        axes[0,0].set_ylabel('Revenue (£)')
        axes[0,0].tick_params(axis='x', rotation=45)
        axes[0,0].grid(True, alpha=0.3)
        
        # Add revenue annotations
        for i, row in monthly_revenue.iterrows():
            if i % 2 == 0:  # Annotate every other point to avoid crowding
                axes[0,0].annotate(f'£{row["revenue"]:,.0f}', 
                                 (row['month'], row['revenue']),
                                 textcoords="offset points", xytext=(0,10), ha='center')
        
        # 2. Customer Lifetime Value Distribution in a histogram
        customer_clv = self.orders.groupby('customer_id')['total_amount'].sum()
        
        axes[0,1].hist(customer_clv, bins=30, alpha=0.7, color='#A23B72', edgecolor='black')
        axes[0,1].axvline(customer_clv.mean(), color='red', linestyle='--', 
                         label=f'Mean: £{customer_clv.mean():.2f}')
        axes[0,1].axvline(customer_clv.median(), color='orange', linestyle='--', 
                         label=f'Median: £{customer_clv.median():.2f}')
        axes[0,1].set_title('💎 Customer Lifetime Value Distribution', fontweight='bold')
        axes[0,1].set_xlabel('Total Spent (£)')
        axes[0,1].set_ylabel('Number of Customers')
        axes[0,1].legend()
        axes[0,1].grid(True, alpha=0.3)
        
        # 3. Revenue by Location in a horizontal bar chart
        location_revenue = self.orders.merge(self.customers, on='customer_id').groupby('location')['total_amount'].sum().sort_values(ascending=True)
        
        bars = axes[1,0].barh(location_revenue.index, location_revenue.values, color='#F18F01')
        axes[1,0].set_title('🌍 Revenue by Location', fontweight='bold')
        axes[1,0].set_xlabel('Total Revenue (£)')
        
        # Add value labels on bars
        for bar in bars:
            width = bar.get_width()
            axes[1,0].text(width, bar.get_y() + bar.get_height()/2, 
                          f'£{width:,.0f}', ha='left', va='center')
        
        # 4. Order Value Distribution by Customer Segment in a boxplot
        order_segment_data = self.orders.merge(self.customers, on='customer_id')
        
        sns.boxplot(data=order_segment_data, x='customer_segment', y='total_amount', ax=axes[1,1])
        axes[1,1].set_title('📦 Order Value by Customer Segment', fontweight='bold')
        axes[1,1].set_xlabel('Customer Segment')
        axes[1,1].set_ylabel('Order Value (£)')
        
        plt.tight_layout()
        plt.savefig('revenue_dashboard.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        return monthly_revenue, customer_clv
    
    def create_customer_analytics(self):
        """Create customer behaviour analysis"""
        fig, axes = plt.subplots(2, 2, figsize=self.fig_size)
        fig.suptitle('👥 Customer Analytics Dashboard', fontsize=16, fontweight='bold')
        
        # 1. Customer Segmentation in a pie chart
        segment_counts = self.customers['customer_segment'].value_counts()
        colors = ['#FF6B6B', '#4ECDC4', '#45B7D1']
        
        wedges, texts, autotexts = axes[0,0].pie(segment_counts.values, labels=segment_counts.index, 
                                                autopct='%1.1f%%', colors=colors, startangle=90)
        axes[0,0].set_title('🎯 Customer Segmentation', fontweight='bold')
        
        # 2. Customer Acquisition Over Time in a line chart
        monthly_signups = self.customers.groupby(self.customers['signup_date'].dt.to_period('M')).size()
        
        axes[0,1].plot(monthly_signups.index.astype(str), monthly_signups.values, 
                      marker='s', linewidth=2, markersize=4, color='#96CEB4')
        axes[0,1].set_title('📈 Monthly Customer Acquisition', fontweight='bold')
        axes[0,1].set_xlabel('Month')
        axes[0,1].set_ylabel('New Customers')
        axes[0,1].tick_params(axis='x', rotation=45)
        axes[0,1].grid(True, alpha=0.3)
        
        # 3. Orders per Customer Distribution in a histogram
        orders_per_customer = self.orders.groupby('customer_id').size()
        
        axes[1,0].hist(orders_per_customer, bins=range(1, orders_per_customer.max()+2), 
                      alpha=0.7, color='#FFEAA7', edgecolor='black')
        axes[1,0].set_title('🛒 Orders per Customer', fontweight='bold')
        axes[1,0].set_xlabel('Number of Orders')
        axes[1,0].set_ylabel('Number of Customers')
        axes[1,0].grid(True, alpha=0.3)
        
        # 4. Customer Value Heatmap by Location and Segment in a heatmap
        customer_summary = self.customers.merge(
            self.orders.groupby('customer_id')['total_amount'].sum().reset_index(),
            on='customer_id', how='left'
        ).fillna(0)
        
        heatmap_data = customer_summary.pivot_table(
            values='total_amount', 
            index='location', 
            columns='customer_segment', 
            aggfunc='mean'
        )
        
        sns.heatmap(heatmap_data, annot=True, fmt='.0f', cmap='YlOrRd', ax=axes[1,1])
        axes[1,1].set_title('🔥 Avg Customer Value Heatmap', fontweight='bold')
        axes[1,1].set_xlabel('Customer Segment')
        axes[1,1].set_ylabel('Location')
        
        plt.tight_layout()
        plt.savefig('customer_analytics.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def create_product_analytics(self):
        """Create product performance analysis"""
        fig, axes = plt.subplots(2, 2, figsize=self.fig_size)
        fig.suptitle('📦 Product Analytics Dashboard', fontsize=16, fontweight='bold')
        
        # 1. Category Performance
        category_stats = self.products.groupby('category').agg({
            'price': 'mean',
            'profit_margin': 'mean',
            'product_id': 'count'
        }).round(2)
        category_stats.columns = ['avg_price', 'avg_margin', 'product_count']
        
        x = np.arange(len(category_stats))
        width = 0.35
        
        bars1 = axes[0,0].bar(x - width/2, category_stats['avg_price'], width, 
                             label='Avg Price', color='#74B9FF')
        bars2 = axes[0,0].bar(x + width/2, category_stats['avg_margin'], width, 
                             label='Avg Margin %', color='#FD79A8')
        
        axes[0,0].set_title('💼 Category Performance', fontweight='bold')
        axes[0,0].set_xlabel('Category')
        axes[0,0].set_ylabel('Value')
        axes[0,0].set_xticks(x)
        axes[0,0].set_xticklabels(category_stats.index, rotation=45)
        axes[0,0].legend()
        axes[0,0].grid(True, alpha=0.3)
        
        # 2. Price Distribution in a histogram
        axes[0,1].hist(self.products['price'], bins=20, alpha=0.7, color='#00B894', edgecolor='black')
        axes[0,1].axvline(self.products['price'].mean(), color='red', linestyle='--', 
                         label=f'Mean: £{self.products["price"].mean():.2f}')
        axes[0,1].set_title('💰 Product Price Distribution', fontweight='bold')
        axes[0,1].set_xlabel('Price (£)')
        axes[0,1].set_ylabel('Number of Products')
        axes[0,1].legend()
        axes[0,1].grid(True, alpha=0.3)
        
        # 3. Profit Margin by Category in a boxplot
        sns.boxplot(data=self.products, x='category', y='profit_margin', ax=axes[1,0])
        axes[1,0].set_title('📊 Profit Margin by Category', fontweight='bold')
        axes[1,0].set_xlabel('Category')
        axes[1,0].set_ylabel('Profit Margin (%)')
        axes[1,0].tick_params(axis='x', rotation=45)
        
        # 4. Price vs Profit Margin Scatter
        for category in self.products['category'].unique():
            cat_data = self.products[self.products['category'] == category]
            axes[1,1].scatter(cat_data['price'], cat_data['profit_margin'], 
                            label=category, alpha=0.7, s=50)
        
        axes[1,1].set_title('💡 Price vs Profit Margin', fontweight='bold')
        axes[1,1].set_xlabel('Price (£)')
        axes[1,1].set_ylabel('Profit Margin (%)')
        axes[1,1].legend()
        axes[1,1].grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('product_analytics.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def create_summary_metrics(self):
        """Create key metrics summary"""
        # Calculate key metrics
        total_revenue = self.orders['total_amount'].sum()
        total_orders = len(self.orders)
        total_customers = len(self.customers)
        avg_order_value = self.orders['total_amount'].mean()
        
        # Customer metrics
        customer_clv = self.orders.groupby('customer_id')['total_amount'].sum()
        avg_clv = customer_clv.mean()
        
        # Create summary visualisation
        fig, ax = plt.subplots(1, 1, figsize=(12, 8))
        
        metrics = {
            'Total Revenue': f'£{total_revenue:,.2f}',
            'Total Orders': f'{total_orders:,}',
            'Total Customers': f'{total_customers:,}',
            'Avg Order Value': f'£{avg_order_value:.2f}',
            'Avg Customer LTV': f'£{avg_clv:.2f}',
            'Revenue per Customer': f'£{total_revenue/total_customers:.2f}'
        }
        
        # Create a professional metrics display
        ax.axis('off')
        fig.suptitle('📈 Key Business Metrics Summary', fontsize=20, fontweight='bold', y=0.9)
        
        # Create metric boxes
        y_positions = np.linspace(0.7, 0.1, len(metrics))
        colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD']
        
        for i, (metric, value) in enumerate(metrics.items()):
            # Create colored box
            bbox = dict(boxstyle="round,pad=0.3", facecolor=colors[i], alpha=0.7)
            ax.text(0.5, y_positions[i], f'{metric}\n{value}', 
                   transform=ax.transAxes, fontsize=16, fontweight='bold',
                   ha='center', va='center', bbox=bbox)
        
        plt.tight_layout()
        plt.savefig('business_metrics.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        return metrics
    
    def generate_all_visualisations(self):
        """Generate complete visualisation suite"""
        print("🎨 Starting visualisation generation...")
        
        self.load_data()
        
        print("📊 Creating revenue dashboard...")
        self.create_revenue_dashboard()
        
        print("👥 Creating customer analytics...")
        self.create_customer_analytics()
        
        print("📦 Creating product analytics...")
        self.create_product_analytics()
        
        print("📈 Creating summary metrics...")
        self.create_summary_metrics()
        
        print("\n✅ All visualisations created successfully!")
        print("📁 Generated files:")
        print("   - revenue_dashboard.png")
        print("   - customer_analytics.png") 
        print("   - product_analytics.png")
        print("   - business_metrics.png")
    

# Run the visualisation suite
if __name__ == "__main__":
    visualiser = ECommerceVisualiser()
    visualiser.generate_all_visualisations()