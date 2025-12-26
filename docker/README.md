# Docker Deployment Guide – E-Commerce Data Pipeline

This document explains how to deploy and run the E-Commerce Data Pipeline using Docker and Docker Compose.

---

## 1. Prerequisites

### Docker Requirements
- Docker Engine **v20.10+**
- Docker Compose **v2.0+**

Check installation:
```bash
docker --version
docker compose version

```
**System Requirements**
- Minimum 4 GB RAM
- Minimum 10 GB free disk space
**Ports required:**
- 5432 → PostgreSQL
- 8000 (optional API / services)

---
## 2. Quick Start Guide
**Step 1:** Build Docker Images
```
docker compose build
```
**Step 2:** Start Services
```
docker compose up -d
```
**Step 3:** Verify Running Containers
```
docker compose ps
```
**Expected services:**
- postgres
- pipeline_app (or similar)
  
**Step 4:** Run Pipeline Inside Container
```
docker compose exec pipeline_app python scripts/pipeline_orchestrator.py
```

**Step 5:** Access PostgreSQL Database
```
docker compose exec postgres psql -U admin -d ecommerce_db
```
Exit psql
```
\q
```
**Step 6:** View logs
```
docker compose logs -f
```
Specific Services:
```
docker compose logs -f
```
**Step 7:** Stop services
```
docker compose down
```
**Step 8:** Cleanup(Volumes & images)
```
docker compose logs -f pipeline_app
```
---
## 3. Troubleshooting
**Port already in use:**
```
netstat -ano | findstr 5432
```
Stop conflicting service or change port in docker-compose.yml

**Database not ready:**
Wait for postgresql health check
```
docker compose logs postgres
```

**Volume Permission Issues:**
```
docker compose down -v
docker compose up -d
```

**Container fails to start:**
```
docker compose logs pipeline_app
```

**Network Connectivity issues:**
Ensures services are on the same docker network
```
docker network ls
```
---
## 4.Configuration
**Environment variables**

Defined in docker-compose.yml:
- POSTGRES_DB
- POSTGRES_USER
- POSTGRES_PASSWORD
- DB_HOST
- DB_PORT

Volume Mounts
- postgres_data → Persistent database storage
- ./data → Pipeline data files

Network COnfiguration

- Single Docker bridge network
- Internal container communication via service names

---
## 5. Verification Checklist

✔ Containers start successfully

✔ PostgreSQL health check passes

✔ Pipeline runs end-to-end

✔ Logs accessible

✔ Data persists across restarts

---
**Author** - Sanjana Medapati

**Project Name:** E-Commerce Data Pipeline Project
