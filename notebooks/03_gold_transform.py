import pandas as pd

def run_gold_transformation():
    print("=== Step 3: Executing Gold Aggregations ===")
    silver_path = "azure-medallion-lakehouse-pipeline/data/silver/healthcare_cleaned.parquet"
    df = pd.read_parquet(silver_path)

    gold_df = df.groupby(['entity', 'indicator']).agg(
        total_records=('value', 'count'),
        avg_metric_value=('value', 'mean'),
        max_metric_value=('value', 'max'),
        sum_metric_value=('value', 'sum')
    ).reset_index()

    gold_df['status'] = gold_df['avg_metric_value'].apply(lambda x: 'HIGH' if x > 1000 else 'NORMAL')
    out_path = "azure-medallion-lakehouse-pipeline/data/gold/fact_healthcare_summary.csv"
    gold_df.to_csv(out_path, index=False)
    print(f"Gold Aggregation Complete. Saved Summary CSV to: {out_path}\n")

if __name__ == "__main__":
    run_gold_transformation()
