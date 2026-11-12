
import subprocess
import sys
from pathlib import Path
from datetime import datetime

PROJECT_DIR = Path(__file__).resolve().parent
SPARK_JOBS = PROJECT_DIR / "spark_jobs"

JOBS = [
    ("Construction de la couche BRONZE", "build_bronze.py"),
    ("Construction de la couche SILVER", "build_silver.py"),
    ("Construction de la couche GOLD", "build_gold.py"),
    ("Export des résultats GOLD en CSV", "export_gold_csv.py"),
]


def run_step(name, script):
    script_path = SPARK_JOBS / script

    print("\n" + "=" * 60, flush=True)
    print(f"ETAPE : {name}", flush=True)
    print("=" * 60, flush=True)

    if not script_path.exists():
        raise FileNotFoundError(f"Script introuvable : {script_path}")

    subprocess.run(
        [sys.executable, str(script_path)],
        cwd=PROJECT_DIR,
        check=True
    )

    print(f"SUCCES : {name}", flush=True)


def main():
    start = datetime.now()

    print("DEMARRAGE DU PIPELINE OLIST", flush=True)

    try:
        for name, script in JOBS:
            run_step(name, script)

    except subprocess.CalledProcessError as error:
        print(f"\nECHEC DU PIPELINE : code {error.returncode}")
        sys.exit(error.returncode)

    except Exception as error:
        print(f"\nERREUR : {error}")
        sys.exit(1)

    duration = datetime.now() - start

    print("\n" + "=" * 60)
    print("PIPELINE OLIST TERMINE AVEC SUCCES !")
    print(f"Duree : {duration}")
    print("=" * 60)


if __name__ == "__main__":
    main()
