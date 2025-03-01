import polars as pl
import os
import argparse
from datetime import datetime

def get_non_null_columns(df):
    """Return list of columns that have at least one non-null value"""
    return [col for col in df.columns if df[col].null_count() != len(df)]

def analyze_parquet_file(file_path: str):
    """Analyze a single parquet file and print its statistics."""
    filename = os.path.basename(file_path)
    print(f"\n{'='*50}")
    print(f"Analyzing: {filename}")
    print(f"{'='*50}")
    
    # Read the parquet file
    df = pl.read_parquet(file_path)
    
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
        non_null_count = len(df) - df[col].null_count()
        percent_filled = (non_null_count / len(df)) * 100
        print(f"- {col:<30} ({non_null_count:,} rows, {percent_filled:.1f}% filled)")
    
    print("\nSample of data (first 5 rows, only columns with values):")
    print("-----------------------------------------------------")
    # Show only columns that have data
    sample_df = df[non_null_cols].head()
    print(sample_df)

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Analyze a parquet file and display statistics.')
    parser.add_argument('file', help='Path to the parquet file to analyze')
    args = parser.parse_args()
    
    # Verify file exists and is a parquet file
    if not os.path.exists(args.file):
        print(f"Error: File '{args.file}' does not exist")
        return
    
    if not args.file.endswith('.parquet'):
        print(f"Error: File '{args.file}' is not a parquet file")
        return
    
    try:
        analyze_parquet_file(args.file)
    except Exception as e:
        print(f"Error analyzing file: {str(e)}")

if __name__ == "__main__":
    main()
