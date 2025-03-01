import duckdb
import os
import glob
import pandas as pd
from typing import List

# Make pandas output more readable
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 30)

def get_non_null_columns(df: pd.DataFrame) -> List[str]:
    """Return list of columns that have at least one non-null value"""
    return [col for col in df.columns if df[col].notna().any()]

def analyze_parquet_file(file_path: str):
    filename = os.path.basename(file_path)
    print(f"\n{'='*50}")
    print(f"Analyzing: {filename}")
    print(f"{'='*50}")
    
    # Read the parquet file
    df = duckdb.read_parquet(file_path).df()
    
    # Show all columns
    print("\nAll columns in schema:")
    print("--------------------")
    for col in df.columns:
        print(f"- {col}")
    
    # Get columns with non-null values
    non_null_cols = get_non_null_columns(df)
    
    print(f"\nColumns with data ({len(non_null_cols)} out of {len(df.columns)} columns):")
    print("-----------------")
    for col in non_null_cols:
        # Count non-null values
        non_null_count = df[col].notna().sum()
        percent_filled = (non_null_count / len(df)) * 100
        print(f"- {col:<30} ({non_null_count:,} rows, {percent_filled:.1f}% filled)")
    
    print("\nSample of data (first 5 rows, only columns with values):")
    print("-----------------------------------------------------")
    # Show only columns that have data
    sample_df = df[non_null_cols].head()
    print(sample_df.to_string(index=False))

def main():
    # Connect to an in-memory DuckDB database
    con = duckdb.connect()
    
    # Get path to preprocessed-data directory
    preprocessed_dir = os.path.join(os.path.dirname(__file__), "preprocessed-data")
    parquet_files = glob.glob(os.path.join(preprocessed_dir, "*.parquet"))
    
    print(f"Found {len(parquet_files)} parquet files")
    
    # Analyze each parquet file
    for parquet_file in parquet_files:
        analyze_parquet_file(parquet_file)

if __name__ == "__main__":
    main()
