# scripts/clean_old_data.py
import os
from datetime import datetime, timedelta

def cleanup_old_files(base_dir="data", days=7, dry_run=True):
    """
    Cleans files older than N days.
    """
    cutoff = datetime.now() - timedelta(days=days)

    if not os.path.exists(base_dir):
        return 0

    removed = 0
    for root, _, files in os.walk(base_dir):
        for f in files:
            path = os.path.join(root, f)
            if datetime.fromtimestamp(os.path.getmtime(path)) < cutoff:
                if not dry_run:
                    os.remove(path)
                removed += 1
    return removed


if __name__ == "__main__":
    cleanup_old_files()
    print("Cleanup completed")
