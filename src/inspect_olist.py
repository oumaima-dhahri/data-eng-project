
from pathlib import Path
import pandas as pd

# 1. Définir le dossier contenant les CSV
PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_DIR / "data" / "raw"

# 2. Vérifier que le dossier existe
if not DATA_DIR.exists():
    raise FileNotFoundError(f"Dossier introuvable : {DATA_DIR}")

# 3. Récupérer tous les fichiers CSV
csv_files = sorted(DATA_DIR.glob("*.csv"))

if not csv_files:
    raise FileNotFoundError(
        f"Aucun fichier CSV trouvé dans : {DATA_DIR}"
    )

print(f"Nombre de fichiers CSV : {len(csv_files)}")
print("=" * 60)

# 4. Inspecter chaque fichier
for file in csv_files:
    print(f"\nFICHIER : {file.name}")

    try:
        df = pd.read_csv(file)

        # Dimensions
        print(f"Nombre de lignes : {df.shape[0]}")
        print(f"Nombre de colonnes : {df.shape[1]}")

        # Colonnes et types
        print("\nColonnes et types :")
        print(df.dtypes.to_string())

        # Valeurs manquantes
        print("\nValeurs manquantes par colonne :")
        missing = df.isna().sum()
        print(missing[missing > 0].to_string()
              if missing.any() else "Aucune")

        # Doublons exacts
        print(f"\nLignes dupliquées : {df.duplicated().sum()}")

        # Aperçu
        print("\nLes 5 premières lignes :")
        print(df.head().to_string(index=False))

        print("\n" + "-" * 60)

    except Exception as error:
        print(f"Erreur de lecture : {error}")
