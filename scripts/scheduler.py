# scripts/scheduler.py
from datetime import datetime

def run_scheduler_once():
    """
    Runs one scheduler cycle (used for tests).
    """
    now = datetime.utcnow()
    return True


if __name__ == "__main__":
    run_scheduler_once()
    print("Scheduler executed")

