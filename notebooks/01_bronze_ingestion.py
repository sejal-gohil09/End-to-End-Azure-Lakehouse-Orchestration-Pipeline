import os
import urllib.request
import pandas as pd
from datetime import datetime

def run_bronze_ingestion():
    print("=== Step 1: Executing Bronze Ingestion (OWID Hospitalizations) ===")
    raw_data_url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/hospitalizations/covid-hospitalizations.csv"
    bronze_csv_path = "azure-medallion-lakehouse-pipeline/data/bronze/raw_healthcare_data.csv"

    print("Downloading raw COVID-19 hospitalizations dataset...")
    urllib.request.urlretrieve(raw_data_url, bronze_csv_path)

    df = pd.read_csv(bronze_csv_path)
    df['_ingestion_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df['_source_system'] = "OWID_GitHub_API"
    df.to_csv(bronze_csv_path, index=False)
    print(f"Bronze Ingestion Complete. Ingested {len(df):,} raw records into: {bronze_csv_path}\n")

if __name__ == "__main__":
    run_bronze_ingestion()
