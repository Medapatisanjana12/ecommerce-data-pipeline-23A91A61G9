
# E-Commerce Data Pipeline Architecture

## Overview
This document describes the architecture of the E-Commerce Data Analytics Platform, designed for scalable batch analytics and BI reporting.

---

## System Components

### 1. Data Generation Layer
- Generates synthetic e-commerce data using Python Faker
- Output: CSV files (customers, products, transactions, transaction_items)

---

### 2. Data Ingestion Layer
- Loads raw CSV files into PostgreSQL staging schema
- Technology: Python + psycopg2
- Pattern: Batch ingestion

---

### 3. Data Storage Layer

#### Staging Schema
- Exact replica of CSV structure
- Minimal validation
- Temporary storage

#### Production Schema
- Cleaned and normalized data
- Foreign key constraints enforced
- Business rules applied

#### Warehouse Schema
- Star schema optimized for analytics
- Fact table: fact_sales
- Dimension tables: customers, products, date, payment method

---

### 4. Data Processing Layer
- Data quality checks
- Transformations and enrichment
- SCD Type 2 ready design
- Aggregate tables for performance

---

### 5. Data Serving Layer
- Pre-aggregated analytics
- SQL-based analytical queries
- CSV outputs for BI tools

---

### 6. Visualization Layer
- Power BI / Tableau dashboards
- Interactive reports
- 16+ visuals across multiple pages

---

### 7. Orchestration Layer
- Pipeline orchestrator
- Scheduler for daily execution
- Monitoring and alerting

---

## Data Models

### Staging Model
- Raw structure
- Minimal checks

### Production Model
- 3NF normalized
- Referential integrity enforced

### Warehouse Model (Star Schema)
- 4 Dimension tables
- 1 Fact table
- Optimized for read-heavy analytics

---

## Technologies Used

- Python 3.13
- PostgreSQL 13+
- Pandas
- SQLAlchemy
- Faker
- Pytest
- Power BI

---

## Deployment Architecture

- Local PostgreSQL database
- Python-based batch pipeline
- Modular scripts for extensibility
- Can be containerized using Docker
