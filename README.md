# E-Commerce Data Pipeline Project

**Student Name:** Sanjana Medapati  
**Roll Number:** 23A91A61G9  
**Submission Date:** 22-12-2025  

---

# E-Commerce Data Pipeline & Analytics Platform

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
→ BI Dashboard

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

---

## Set up Instructions
### 1.Create Virtual Environment
```
python -m venv venv
source venv/Scripts/activate   # Windows Git Bash

