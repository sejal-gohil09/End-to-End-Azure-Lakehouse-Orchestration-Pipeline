import os
import glob
import pandas as pd

def run_silver_transformation():
    print("==================================================")
    print("   SILVER LAYER: DATASET PROFILING & CLEANING    ")
    print("==================================================")

    bronze_dir = "azure-medallion-lakehouse-pipeline/data/bronze"
    csv_files = glob.glob(f"{bronze_dir}/*.csv")

    if not csv_files:
        raise FileNotFoundError(f"No raw files found in {bronze_dir}!")

    df = pd.read_csv(csv_files[0], low_memory=False)

    total_rows, total_cols = df.shape
    print(f"-> Total Dataset Rows    : {total_rows:,}")
    print(f"-> Total Dataset Columns : {total_cols}")
    print("-" * 50)

    print("-> Dataset Column Names:")
    for col in df.columns:
        print(f"   - {col}")
    print("-" * 50)

    df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
    df = df.dropna(subset=['entity', 'indicator', 'value']).drop_duplicates()
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['value'] = pd.to_numeric(df['value'], errors='coerce').fillna(0)
    df['dq_value_flag'] = df['value'].apply(lambda x: 'VALID' if x >= 0 else 'INVALID')
    df = df[df['dq_value_flag'] == 'VALID']

    print(f"-> Total Dataset Value Sum : {df['value'].sum():,.2f}")
    print(f"-> Average Indicator Value : {df['value'].mean():,.2f}")
    print("-" * 50)

    print("-> Preview First 10 Rows:")
    print(df.head(10).to_string())
    print("==================================================")

    out_path = "azure-medallion-lakehouse-pipeline/data/silver/healthcare_cleaned.parquet"
    df.to_parquet(out_path, index=False)
    print(f"Silver Transformation Complete. Saved Parquet to: {out_path}\n")

if __name__ == "__main__":
    run_silver_transformation()
