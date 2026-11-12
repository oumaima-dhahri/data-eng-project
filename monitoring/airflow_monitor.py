import csv
from pathlib import Path
from datetime import datetime, timezone

REPORT_DIR = Path(__file__).parent / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

REPORT_FILE = REPORT_DIR / "airflow_task_history.csv"

COLUMNS = [
    "timestamp",
    "dag_id",
    "run_id",
    "task_id",
    "state",
    "duration_seconds",
    "error"
]


def record_task(context):
    task_instance = context["ti"]

    now = datetime.now(timezone.utc)
    start_date = getattr(task_instance, "start_date", None)

    duration = ""

    if start_date:
        try:
            if start_date.tzinfo is None:
                start_date = start_date.replace(tzinfo=timezone.utc)

            duration = round(
                (now - start_date).total_seconds(), 2
            )
        except (TypeError, AttributeError):
            pass

    row = {
        "timestamp": now.isoformat(),
        "dag_id": task_instance.dag_id,
        "run_id": task_instance.run_id,
        "task_id": task_instance.task_id,
        "state": str(task_instance.state or ""),
        "duration_seconds": duration,
        "error": str(context.get("exception") or "")
    }

    file_exists = REPORT_FILE.exists()

    with open(REPORT_FILE, "a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=COLUMNS)

        if not file_exists:
            writer.writeheader()

        writer.writerow(row)

    print(f"Monitoring report updated: {REPORT_FILE}")
