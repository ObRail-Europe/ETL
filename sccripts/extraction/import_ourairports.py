import os
import json
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from io import StringIO


load_dotenv()

BASE_URL = os.getenv("OURAIRPORTS_BASE_URL")
RAW_PATH = os.getenv("DATA_RAW_PATH")
DATA_VERSION = os.getenv("DATA_VERSION")

FILES = {
    "airports": "airports.csv",
    "countries": "countries.csv",
    "regions": "regions.csv"
}

os.makedirs(RAW_PATH, exist_ok=True)


def download_csv(file_name: str) -> pd.DataFrame:
    url = f"{BASE_URL}/{file_name}"
    response = requests.get(url)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))



def validate_airports_schema(df: pd.DataFrame):
    required_columns = {
        "ident",
        "type",
        "name",
        "latitude_deg",
        "longitude_deg",
        "iso_country"
    }

    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Colonnes manquantes : {missing}")



def run():
    metadata = {
        "source": "OurAirports",
        "source_url": BASE_URL,
        "version": DATA_VERSION,
        "ingestion_date": datetime.utcnow().isoformat(),
        "files": []
    }

    for dataset, file_name in FILES.items():
        print(f" Téléchargement {file_name}")
        df = download_csv(file_name)

        if dataset == "airports":
            validate_airports_schema(df)

        output_path = os.path.join(RAW_PATH, file_name)
        df.to_csv(output_path, index=False)

        metadata["files"].append({
            "file": file_name,
            "rows": len(df)
        })

    
    metadata_path = os.path.join(RAW_PATH, "metadata.json")
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    print(" Extraction OurAirports terminée")


if __name__ == "__main__":
    run()
