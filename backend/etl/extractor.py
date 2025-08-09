import csv
import json
from pathlib import Path
import pandas as pd


def extract_data(file_path):
    file_path = Path(file_path)
    file_type = file_path.suffix

    try:
        # ✅ Check if file exists
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # ✅ Handle CSV
        if file_type == ".csv":
            print(f"Extracting CSV data from {file_path}")
            df = pd.read_csv(file_path)
        
        # ✅ Handle JSON
        elif file_type == ".json":
            print(f"Extracting JSON data from {file_path}")
            df = pd.read_json(file_path)
        
        # ❌ Unsupported file type
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

        # ✅ Show a preview
        print(df.head())
        
        # ✅ Return the DataFrame
        return df

    except FileNotFoundError as fnf:
        print(f"[ERROR] {fnf}")
        raise fnf

    except pd.errors.EmptyDataError as ede:
        print(f"[ERROR] File is empty: {file_path}")
        raise ede

    except pd.errors.ParserError as pe:
        print(f"[ERROR] Could not parse file: {file_path} — Possibly corrupted CSV")
        raise pe

    except ValueError as ve:
        print(f"[ERROR] {ve}")
        raise ve

    except json.JSONDecodeError as jde:
        print(f"[ERROR] Malformed JSON in file: {file_path}")
        raise jde

    except Exception as e:
        print(f"[CRITICAL] Unexpected error: {e}")
        raise e
