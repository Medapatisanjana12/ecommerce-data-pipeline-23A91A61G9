import json
import psycopg2
from datetime import datetime, timezone
import statistics

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "ecommerce_db",
    "user": "admin",
    "password": "admin"
}

REPORT_PATH = "data/processed/monitoring_report.json"

def run_query(sql):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()
    return rows

def main():
    report = {
        "monitoring_timestamp": datetime.now(timezone.utc).isoformat(),
        "pipeline_health": "healthy",
        "alerts": []
    }

    # -----------------------
    # 1. Data Freshness
    # -----------------------
    freshness_sql = """
    SELECT 'staging' AS layer, MAX(transaction_date) AS latest_record FROM staging.transactions
    UNION ALL
    SELECT 'production' AS layer, MAX(transaction_date) AS latest_record FROM production.transactions;
    """
    freshness = run_query(freshness_sql)

    report["data_freshness"] = {
        layer: str(ts) if ts else None
        for layer, ts in freshness
    }

    # -----------------------
    # 2. Volume Anomaly
    # -----------------------
    volume_sql = """
    SELECT COUNT(*)
    FROM production.transactions
    WHERE transaction_date = CURRENT_DATE;
    """
    today_count = run_query(volume_sql)[0][0]

    report["data_volume"] = {
        "today_count": today_count,
        "status": "ok" if today_count > 0 else "critical"
    }

    if today_count == 0:
        report["alerts"].append({
            "severity": "critical",
            "message": "Zero transactions today"
        })
        report["pipeline_health"] = "critical"

    # -----------------------
    # 3. Data Quality
    # -----------------------
    quality_sql = """
    SELECT
        COUNT(*) FILTER (WHERE customer_id IS NULL),
        COUNT(*) FILTER (WHERE total_amount <= 0)
    FROM production.transactions;
    """
    nulls, invalids = run_query(quality_sql)[0]

    report["data_quality"] = {
        "null_customer_ids": nulls,
        "invalid_amounts": invalids
    }

    if nulls > 0 or invalids > 0:
        report["alerts"].append({
            "severity": "warning",
            "message": "Data quality issues detected"
        })
        report["pipeline_health"] = "degraded"

    # -----------------------
    # Save Report
    # -----------------------
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print("Monitoring report generated successfully")

if __name__ == "__main__":
    main()
