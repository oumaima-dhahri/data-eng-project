
from pathlib import Path
from getpass import getpass

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# 1. Chemins du projet
PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_DIR / "data" / "raw"

# 2. Paramètres PostgreSQL
password = getpass("PostgreSQL password: ")

url = URL.create(
    drivername="postgresql+psycopg",
    username="postgres",
    password=password,
    host="localhost",
    port=5433,
    database="olist_dw",
)

engine = create_engine(url)

# 3. Vérifier la connexion et créer le schéma raw
try:
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.execute(text("SELECT 1"))

    print("Connexion PostgreSQL réussie.")
    print("Schéma raw prêt.")

    # 4. Repérer les fichiers CSV
    csv_files = sorted(DATA_DIR.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(
            f"Aucun CSV trouvé dans {DATA_DIR}"
        )

    print(f"{len(csv_files)} fichiers CSV détectés.")

    # 5. Importer chaque CSV par blocs
    for file in csv_files:
        table_name = file.stem.replace("olist_", "")
        print(f"\nImportation : {file.name}")

        first_chunk = True
        total_rows = 0

        for chunk in pd.read_csv(
            file,
            chunksize=10000,
        ):
            chunk.to_sql(
                name=table_name,
                con=engine,
                schema="raw",
                if_exists="replace" if first_chunk else "append",
                index=False,
                chunksize=2000,
                method="multi",
            )

            total_rows += len(chunk)
            first_chunk = False

        print(f"Table : raw.{table_name}")
        print(f"Lignes importées : {total_rows}")

    print("\nImportation terminée !")

finally:
    engine.dispose()
