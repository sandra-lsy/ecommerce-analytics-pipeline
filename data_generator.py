# data_generator.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json

# Set random seed for reproducibility
np.random.seed(42)

def generate_customers(n=1000):
    """Generate realistic customer data"""
    return pd.DataFrame({
        'customer_id': range(1, n+1),
        'name': [f'Customer_{i}' for i in range(1, n+1)],
        'email': [f'user{i}@email.com' for i in range(1, n+1)],
        'signup_date': pd.date_range('2023-01-01', periods=n, freq='D'),
        'location': np.random.choice(['London', 'Manchester', 'Birmingham', 'Edinburgh'], n),
        'age': np.random.randint(18, 70, n),
        'customer_segment': np.random.choice(['Premium', 'Standard', 'Basic'], n, p=[0.2, 0.5, 0.3])
    })

def generate_products(n=100):
    """Generate product catalog"""
    categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports']
    products = []
    
    for i in range(1, n+1):
        category = np.random.choice(categories)
        base_price = np.random.uniform(10, 500)
        products.append({
            'product_id': i,
            'name': f'{category}_Product_{i}',
            'category': category,
            'price': round(base_price, 2),
            'cost': round(base_price * 0.6, 2),  # 40% margin
            'stock_quantity': np.random.randint(0, 100)
        })
    
    return pd.DataFrame(products)

def generate_orders(customers, products, n=5000):
    """Generate realistic order data"""
    orders = []
    
    for i in range(1, n+1):
        customer_id = np.random.choice(customers['customer_id'])
        order_date = pd.Timestamp('2023-01-01') + pd.Timedelta(days=np.random.randint(0, 365))
        
        # Number of items in order (1-5 items)
        num_items = np.random.choice([1, 2, 3, 4, 5], p=[0.4, 0.3, 0.2, 0.08, 0.02])
        selected_products = np.random.choice(products['product_id'], num_items, replace=False)
        
        total_amount = 0
        for product_id in selected_products:
            price = products[products['product_id'] == product_id]['price'].iloc[0]
            quantity = np.random.randint(1, 4)
            total_amount += price * quantity
        
        orders.append({
            'order_id': i,
            'customer_id': customer_id,
            'order_date': order_date,
            'total_amount': round(total_amount, 2),
            'status': np.random.choice(['Completed', 'Pending', 'Cancelled'], p=[0.85, 0.1, 0.05])
        })
    
    return pd.DataFrame(orders)

# Generate all data
print("Generating synthetic e-commerce data...")
customers = generate_customers(1000)
products = generate_products(100)
orders = generate_orders(customers, products, 5000)

# Save in different formats for ETL practice
customers.to_csv('data/customers.csv', index=False)
products.to_json('data/products.json', orient='records', indent=2)
orders.to_csv('data/orders.csv', index=False)

print("✅ Data generated successfully!")
print(f"- {len(customers)} customers")
print(f"- {len(products)} products") 
print(f"- {len(orders)} orders")