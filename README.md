# E-Commerce Data Pipeline Project

**Student Name:** Sanjana Medapati  
**Roll Number:** 23A91A61G9  
**Submission Date:** 27-12-2025  

---

## Project Overview
This project implements an end-to-end **E-Commerce Data Analytics Pipeline** that generates synthetic data, ingests it into PostgreSQL, transforms it into analytical models, and produces business-ready analytics for visualization in BI tools.

The pipeline follows a **Raw → Staging → Production → Warehouse → Analytics** architecture and supports automation, monitoring, testing, and documentation.

---

## Project Architecture

**Data Flow:**

Raw CSV Data  
→ Staging Schema  
→ Production Schema  
→ Warehouse (Star Schema)  
→ Analytics (CSV Outputs)  
→ PowerBI Dashboard

---

## Technology Stack

- **Data Generation:** Python, Faker  
- **Database:** PostgreSQL  
- **ETL & Transformations:** Python, Pandas, SQLAlchemy  
- **Orchestration:** Python Scheduler  
- **Monitoring & Alerting:** Python + SQL  
- **Analytics:** SQL, Pandas  
- **Visualization:** Power BI / Tableau  
- **Testing:** Pytest  
- **Version Control:** Git  

---

## Project Structure

```text
ecommerce-data-pipeline/
│
├── config/
│   └── config.yaml
│
├── data/
│   ├── raw/
│   ├── processed/
│   │   ├── analytics/
│   │   ├── monitoring_report.json
│   │   └── pipeline_execution_report.json
│
├── scripts/
│   ├── data_generation/
│   ├── ingestion/
│   ├── transformation/
│   ├── monitoring/
│   ├── scheduler.py
│   ├── clean_old_data.py
│   └── pipeline_orchestrator.py
│
├── sql/
│   └── queries/
│
├── tests/
│   ├── test_data_generation.py
│   ├── test_ingestion.py
│   ├── test_transformation.py
│   ├── test_warehouse.py
│
├── docs/
│   └── architecture.md
│
├── pytest.ini
└── README.md
``` 
---

## Set up Instructions

### 1.Create Virtual Environment
```
python -m venv venv
source venv/Scripts/activate   # Windows Git Bash
```
### 2.Install Dependencies
```
pip install -r requirements.txt
```
### 3.PostgreSQL setup
 Create database: ecommerce_db
 Ensure schemas exist:
- staging
- production
- warehouse

---

## Running the Pipeline
### Full Pipeline Execution
```
python scripts/pipeline_orchestrator.py
```
### Individual steps
```
python scripts/data_generation/generate_data.py
python scripts/ingestion/load_to_staging.py
python scripts/transformation/staging_to_production.py
python scripts/transformation/build_warehouse.py
python scripts/transformation/generate_analytics.py
```
---
## Running Tests
```
pytest
```
With coverage
```
pytest --cov=scripts
```
---
## Analytics Output
Generated at:

data/processed/analytics/

Includes:
- Top Products
- Monthly Sales Trend
- Customer Segmentation
- Category Performance
- Payment Method Distribution
- Geographic Analysis
- Customer Lifetime Value
- Product Profitability
- Day of Week Pattern
- Discount Impact

---
## Dashboard Access
- **Power BI**: dashboards/powerbi/ecommerce_analytics.pbix
- **Screenshots** : dashboards/screenshots/

---
## Database Schemas
**Staging Schema**
- staging.customers
- staging.products
- staging.transactions
- staging.transaction_items
  
**Production Schema**
- production.customers
- production.products
- production.transactions
- production.transaction_items
  
**Warehouse Schema**
- warehouse.dim_customers
- warehouse.dim_products
- warehouse.dim_date
- warehouse.dim_payment_method
- warehouse.fact_sales

---
## Key Insights from Analytics
- Top revenue-generating products
- Monthly sales growth trends
- Customer spending segments
- State-wise revenue distribution
- Payment method preferences

---
## Challenges & Solutions
- **Schema mismatch issues:** Solved by aligning queries with actual table structures
- **Encoding errors:** Fixed by removing Unicode characters
- **Idempotency:** Ensured via truncation-based reloads
- **Test failures:** Fixed with referential integrity validation

--- 
## Future Enhancements
- Real-time streaming using Kafka
- Cloud deployment (AWS / Azure)
- Advanced ML-based demand forecasting
- Real-time alerting system

---
## Contact
**Name:** Sanjana Medapati

**Roll Number:** 23A91A61G9

**Project:** E-Commerce Data Analytics Pipeline

**Email:** 23a91a61g9@aec.edu.in
