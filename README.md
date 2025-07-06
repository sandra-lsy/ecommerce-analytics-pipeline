# E-Commerce Analytics Pipeline

A comprehensive data engineering and analytics project demonstrating ETL pipeline development, SQL analytics, and data visualisation skills.

## 🚀 Project Overview

This project showcases a complete data analytics workflow for e-commerce business intelligence, featuring:

- **Multi-source ETL Pipeline** with data quality validation
- **Advanced SQL Analytics** with complex business queries
- **Python Data Visualisations** using matplotlib and seaborn
- **Automated Business Insights** generation

## 📊 Key Features

### ETL Pipeline
- ✅ **Multi-format data extraction** (CSV, JSON)
- ✅ **Data transformation** with type conversion and derived columns
- ✅ **Quality validation** with missing value and duplicate detection
- ✅ **SQLite database loading** with proper indexing
- ✅ **Error handling** and comprehensive logging

### SQL Analytics
- ✅ **Complex queries** using CTEs and window functions
- ✅ **Business metrics** calculation (CLV, growth rates, segmentation)
- ✅ **Geographic analysis** and customer segmentation
- ✅ **Advanced showcases** including cohort analysis

### Data Visualisation
- ✅ **Revenue analytics** dashboard with trend analysis
- ✅ **Customer behavior** analysis and segmentation
- ✅ **Product performance** metrics and category analysis
- ✅ **Executive summary** with key business metrics

## 🛠️ Technical Stack

- **Python**: pandas, numpy, matplotlib, seaborn
- **Database**: SQLite with complex analytical queries
- **Data Processing**: ETL pipeline with error handling
- **Visualisation**: Dashboards and charts

## 📁 Project Structure

```
ecommerce-analytics-pipeline/
├── src/
│   ├── etl_pipeline.py          # ETL pipeline with data quality checks
│   ├── sql_analysis.py          # Working SQL queries integrated with Python
│   └── visualisation.py        # Data visualisation suite
├── sql/
│   └── business_queries.sql     # Advanced SQL showcase (complex analytics)
├── data/
│   ├── customers.csv            # Customer data
│   ├── products.json            # Product catalog
│   └── orders.csv               # Order transactions
├── data_generator.py            # Synthetic data generation
├── ecommerce.db                 # SQLite database
├── *.png                        # Generated visualisations
├── requirements.txt             # Project dependencies
├── .gitignore                   # Git ignore patterns
└── README.md                    # Project documentation
```

## 🔍 SQL Implementation Details

This project demonstrates SQL capabilities through two complementary approaches:

### `sql/business_queries.sql` - Advanced SQL Showcase
- **Purpose**: Demonstrates complex SQL thinking and advanced analytics capabilities
- **Features**: 
  - CTEs (Common Table Expressions) with multiple levels
  - Window functions (LAG, FIRST_VALUE, ROW_NUMBER)
  - Advanced customer segmentation logic
  - Cohort analysis with retention calculations
  - Growth rate calculations using window functions
- **Note**: These queries showcase SQL expertise but may require additional data model complexity

### `src/sql_analysis.py` - Production Pipeline Queries
- **Purpose**: Working SQL queries integrated with Python pipeline
- **Features**:
  - Simplified, reliable queries that match current data structure
  - Integrated with Python for automated execution
  - Error handling and result processing
  - Business metrics calculation
- **Note**: These queries actually run in the pipeline and produce results

**Both files serve different purposes:**
- `business_queries.sql` shows **what you can do** with advanced SQL
- `sql_analysis.py` shows **what actually works** in the current pipeline

## 🚀 Quick Start

### Prerequisites
```bash
pip install -r requirements.txt
```

### Run the Complete Pipeline
```bash
# 1. Generate synthetic data
python data_generator.py

# 2. Run ETL pipeline
python src/etl_pipeline.py

# 3. Execute SQL analysis
python src/sql_analysis.py

# 4. Create visualisations
python src/visualisation.py
```

## 📈 Generated Outputs

### Visualisations
- **revenue_dashboard.png**: Monthly trends and customer lifetime value
- **customer_analytics.png**: Segmentation and acquisition analysis
- **product_analytics.png**: Category performance and pricing analysis
- **business_metrics.png**: Executive summary dashboard

### Database
- **ecommerce.db**: SQLite database with indexed tables
- **Complex queries**: Available in `sql/business_queries.sql`

## 🎯 Business Insights Demonstrated

### Revenue Analytics
- Monthly revenue trends and performance tracking
- Customer lifetime value distribution analysis
- Geographic revenue performance comparison

### Customer Intelligence
- Behavioral segmentation (Premium, Standard, Basic)
- Acquisition trends and retention patterns
- Order frequency and value analysis

### Product Performance
- Category profitability analysis
- Price optimisation insights
- Inventory and margin analysis

## 🔧 Technical Highlights

### ETL Pipeline
```python
# Professional error handling and logging
logger.info("Starting ETL Pipeline...")
try:
    self.extract_data()
    self.transform_data()
    self.load_data()
except Exception as e:
    logger.error(f"Pipeline failed: {e}")
```

### Advanced SQL
```sql
-- Customer segmentation with CTEs
WITH customer_metrics AS (
    SELECT customer_id, SUM(total_amount) as lifetime_value
    FROM orders WHERE status = 'Completed'
    GROUP BY customer_id
)
SELECT * FROM customer_metrics;
```

### Professional Visualisations
```python
# Multi-panel dashboards with business context
fig, axes = plt.subplots(2, 2, figsize=(15, 12))
fig.suptitle('E-Commerce Analytics Dashboard')
```

## 📊 Key Metrics Generated

- **Total Revenue**: £4,064,556.55
- **Average Customer Lifetime Value**: £4,101.47 average
- **Average Order Value**: £962.03
- **Customer Segments**: Premium (20.6%), Standard (49.3%), Basic (30.1%)

## 🎓 Skills Demonstrated

### Data Engineering
- ETL pipeline design and implementation
- Data quality validation and error handling
- Database schema design and optimisation

### SQL Analytics
- Complex query writing (CTEs, window functions)
- Business intelligence and KPI calculation
- Performance optimisation with indexing

### Data Visualisation
- Professional dashboard creation
- Business storytelling through data
- Multiple chart types and statistical analysis

### Software Engineering
- Clean, modular code structure
- Comprehensive documentation
- Error handling and logging
- Version control best practices

## 🚀 Future Enhancements

- [ ] Add real-time data streaming capabilities
- [ ] Implement automated alert system
- [ ] Create interactive web dashboard
- [ ] Add machine learning predictions
- [ ] Integrate with cloud platforms (AWS/GCP)
