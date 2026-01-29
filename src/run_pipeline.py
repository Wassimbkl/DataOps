import os
import sys
import argparse

from pipeline.ingest import load_data
from pipeline.clean import clean_data
from pipeline.validate import validate_data


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data/raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data/processed")


def run(input_file: str):
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    input_path = os.path.join(BASE_DIR, input_file)

    # ❌ FICHIER INEXISTANT → ÉCHEC
    if not os.path.exists(input_path):
        print(f"❌ ERREUR : fichier introuvable → {input_path}")
        sys.exit(1)

    filename = os.path.basename(input_path)
    output_filename = filename.replace("dirty", "clean")
    output_path = os.path.join(PROCESSED_DIR, output_filename)

    print(f"▶️ Processing {filename}...")

    df = load_data(input_path)
    df_clean = clean_data(df)
    validate_data(df_clean)
    df_clean.to_csv(output_path, index=False)

    print(f"✅ Fichier traité avec succès : {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run data pipeline")
    parser.add_argument("--input", required=True, help="Chemin du fichier CSV brut")
    args = parser.parse_args()

    run(args.input)
