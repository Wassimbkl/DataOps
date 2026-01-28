import os
from pipeline.ingest import load_data
from pipeline.clean import clean_data
from pipeline.validate import validate_data


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data/raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data/processed")

def run():
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    for filename in os.listdir(RAW_DIR):
        if not filename.endswith(".csv"):
            continue

        input_path = os.path.join(RAW_DIR, filename)
        output_filename = filename.replace("dirty", "clean")
        output_path = os.path.join(PROCESSED_DIR, output_filename)

        print(f"Processing {filename}...")

        df = load_data(input_path)
        df_clean = clean_data(df)
        validate_data(df_clean)
        df_clean.to_csv(output_path, index=False)

if __name__ == "__main__":
    run()
