import psycopg2
import yaml
import json
from datetime import datetime

REPORT_PATH = "data/quality_reports/data_quality_report.json"

# -------------------------------
# Load config
# -------------------------------
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

db = config["database"]

# -------------------------------
# DB Connection
# -------------------------------
conn = psycopg2.connect(
    host=db["host"],
    port=db["port"],
    dbname=db["name"],
    user=db["user"],
    password=db["password"]
)
cursor = conn.cursor()

# -------------------------------
# Run quality checks
# -------------------------------
cursor.execute(open("sql/queries/data_quality_checks.sql").read())

results = cursor.fetchall()

checks = {}
total_violations = 0

for check_name, violations in results:
    checks[check_name] = violations
    total_violations += violations

# -------------------------------
# Scoring Logic
# -------------------------------
if total_violations == 0:
    score = 100
    grade = "A"
elif total_violations <= 5:
    score = 90
    grade = "B"
elif total_violations <= 20:
    score = 75
    grade = "C"
elif total_violations <= 50:
    score = 60
    grade = "D"
else:
    score = 40
    grade = "F"

# -------------------------------
# Final Report
# -------------------------------
report = {
    "check_timestamp": datetime.now().isoformat(),
    "checks_performed": checks,
    "total_violations": total_violations,
    "overall_quality_score": score,
    "quality_grade": grade
}

with open(REPORT_PATH, "w") as f:
    json.dump(report, f, indent=4)

cursor.close()
conn.close()

print("Data quality validation completed")
print(f"Score: {score}, Grade: {grade}")
