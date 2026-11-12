
from pathlib import Path
import pandas as pd

# Dossier qui contient les fichiers CSV
data_folder = Path("data/raw")

# Vérifier si le dossier existe
if not data_folder.exists():
    print("Erreur : le dossier data/raw n'existe pas.")
else:
    csv_files = list(data_folder.glob("*.csv"))

    print(f"Nombre de fichiers CSV : {len(csv_files)}")

    for file in csv_files:
        print(f"\n--- Fichier : {file.name} ---")

        try:
            df = pd.read_csv(file)

            print(f"Nombre de lignes : {len(df)}")
            print(f"Nombre de colonnes : {len(df.columns)}")
            print("Colonnes :", list(df.columns))
            print("\nExemple de données :")
            print(df.head(3))

        except Exception as e:
            print(f"Erreur de lecture : {e}")