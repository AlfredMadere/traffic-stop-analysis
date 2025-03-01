import duckdb
import os
import glob

def main():
    # Get all parquet files in preprocessed-data directory
    preprocessed_dir = os.path.join(os.path.dirname(__file__), "preprocessed-data")
    parquet_files = glob.glob(os.path.join(preprocessed_dir, "*.parquet"))
    
    if not parquet_files:
        print("No parquet files found in preprocessed-data directory")
        return
    
    print(f"Found {len(parquet_files)} parquet files")
    
    # Connect to an in-memory DuckDB database
    con = duckdb.connect()
    
    try:
        # First verify we can read the files
        test_df = con.execute("SELECT * FROM read_parquet('preprocessed-data/*.parquet') LIMIT 1").df()
        print("\nColumns in the data:")
        for col in test_df.columns:
            print(f"- {col}")
        
        # Now create the view and run analysis
        con.execute("CREATE VIEW all_stops AS SELECT * FROM read_parquet('preprocessed-data/*.parquet')")
        
        # Get total row count
        total_rows = con.execute("SELECT COUNT(*) FROM all_stops").fetchall()[0][0]
        
        # Get count of rows with valid race data
        valid_race_rows = con.execute("""
            SELECT COUNT(*) as valid_race_rows
            FROM all_stops
            WHERE subject_race IS NOT NULL 
            AND LOWER(subject_race) NOT IN ('unknown', 'other')
        """).fetchall()[0][0]
        
        # Print results
        print(f"\nData Quality Analysis")
        print("=" * 80)
        print(f"Total rows across all files: {total_rows:,}")
        print(f"Rows with valid race data: {valid_race_rows:,}")
        print(f"Percentage of rows with valid race data: {(valid_race_rows/total_rows)*100:.1f}%")
        
        # Show distribution of race values
        print("\nRace Distribution:")
        print("-" * 80)
        race_dist = con.execute("""
            SELECT 
                COALESCE(subject_race, 'NULL') as race,
                COUNT(*) as count,
                (COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()) as percentage
            FROM all_stops
            GROUP BY subject_race
            ORDER BY count DESC
        """).fetchall()
        
        print(f"{'Race':<20} {'Count':<15} {'Percentage':<10}")
        print("-" * 80)
        for race, count, pct in race_dist:
            print(f"{race:<20} {count:<15,} {pct:.1f}%")
            
    except Exception as e:
        print(f"Error analyzing data: {str(e)}")
        print("Verify that:")
        print("1. The preprocessed-data directory exists")
        print("2. There are .parquet files in the directory")
        print("3. The parquet files are readable")
        print("\nDirectory contents:")
        for f in parquet_files:
            print(f"- {os.path.basename(f)}")

if __name__ == "__main__":
    main()
