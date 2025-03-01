import polars as pl
import pandas as pd
import os
import dotenv
import time
from datetime import datetime, timedelta
import tempfile
from uuid import uuid4
import glob

_ = dotenv.load_dotenv()

# Schema definitions for consistent type handling
SCHEMA_OVERRIDES = {
    # String/Text columns
    "raw_row_number": pl.Utf8,
    "location": pl.Utf8,
    "geocode_source": pl.Utf8,
    "county_name": pl.Utf8,
    "neighborhood": pl.Utf8,
    "beat": pl.Utf8,  # Could be numeric but often contains letters
    "district": pl.Utf8,
    "subdistrict": pl.Utf8,
    "division": pl.Utf8,
    "subdivision": pl.Utf8,
    "police_grid_number": pl.Utf8,
    "precinct": pl.Utf8,
    "region": pl.Utf8,
    "reporing_area": pl.Utf8,
    "sector": pl.Utf8,
    "subsector": pl.Utf8,
    "substation": pl.Utf8,
    "service_area": pl.Utf8,
    "zone": pl.Utf8,
    "subject_race": pl.Utf8,
    "subject_sex": pl.Utf8,
    "officer_id_hash": pl.Utf8,
    "officer_race": pl.Utf8,
    "officer_sex": pl.Utf8,
    "officer_assignment": pl.Utf8,
    "department_name": pl.Utf8,
    "unit": pl.Utf8,
    "type": pl.Utf8,
    "disposition": pl.Utf8,
    "violation": pl.Utf8,
    "outcome": pl.Utf8,
    "search_basis": pl.Utf8,
    "reason_for_arrest": pl.Utf8,
    "reason_for_frisk": pl.Utf8,
    "reason_for_search": pl.Utf8,
    "reason_for_stop": pl.Utf8,
    "use_of_force_description": pl.Utf8,
    "use_of_force_reason": pl.Utf8,
    "vehicle_color": pl.Utf8,
    "vehicle_make": pl.Utf8,
    "vehicle_model": pl.Utf8,
    "vehicle_type": pl.Utf8,
    "vehicle_registration_state": pl.Utf8,
    "notes": pl.Utf8,
    
    # Date columns
    "date": pl.Date,
    
    # Time columns
    "time": pl.Time,
    
    # Float columns
    "lat": pl.Float64,
    "lng": pl.Float64,
    "subject_age": pl.Float64,
    "officer_age": pl.Float64,
    "speed": pl.Float64,
    "posted_speed": pl.Float64,
    
    # Integer columns
    "officer_years_of_service": pl.Int64,
    "department_id": pl.Int64,
    "vehicle_year": pl.Int64,
    
    # Boolean columns
    "arrest_made": pl.Boolean,
    "citation_issued": pl.Boolean,
    "warning_issued": pl.Boolean,
    "contraband_found": pl.Boolean,
    "contraband_drugs": pl.Boolean,
    "contraband_weapons": pl.Boolean,
    "contraband_other": pl.Boolean,
    "frisk_performed": pl.Boolean,
    "search_conducted": pl.Boolean,
    "search_person": pl.Boolean,
    "search_vehicle": pl.Boolean
}

# Predefined set of columns that might appear in any CSV file
PREDEFINED_COLUMNS = [
    # Core fields
    "raw_row_number",
    "date",
    "time",
    "location",
    "lat",
    "lng",
    "geocode_source",
    
    # Geographic/Administrative fields
    "county_name",
    "neighborhood",
    "beat",
    "district",
    "subdistrict",
    "division",
    "subdivision",
    "police_grid_number",
    "precinct",
    "region",
    "reporing_area",
    "sector",
    "subsector",
    "substation",
    "service_area",
    "zone",
    
    # Subject information
    "subject_age",
    "subject_race",
    "subject_sex",
    
    # Officer information
    "officer_id_hash",
    "officer_age",
    "officer_race",
    "officer_sex",
    "officer_years_of_service",
    "officer_assignment",
    "department_id",
    "department_name",
    "unit",
    
    # Stop details
    "type",
    "disposition",
    "violation",
    "arrest_made",
    "citation_issued",
    "warning_issued",
    "outcome",
    
    # Search and contraband
    "contraband_found",
    "contraband_drugs",
    "contraband_weapons",
    "contraband_other",
    "frisk_performed",
    "search_conducted",
    "search_person",
    "search_vehicle",
    "search_basis",
    
    # Reasons
    "reason_for_arrest",
    "reason_for_frisk",
    "reason_for_search",
    "reason_for_stop",
    
    # Vehicle information
    "speed",
    "posted_speed",
    "vehicle_color",
    "vehicle_make",
    "vehicle_model",
    "vehicle_type",
    "vehicle_registration_state",
    "vehicle_year",
    
    # Additional fields
    "use_of_force_description",
    "use_of_force_reason",
    "notes"
]

def process_batch(batch: pl.DataFrame, file_id: str):
    # Create unique ID by combining file_id and raw_row_number (preserving any pipe-delimited IDs)
    batch = batch.with_columns(
        (pl.lit(file_id) + "_" + pl.col("raw_row_number")).alias("unique_id")
    )
    
    # Create a list of expressions to handle each predefined column
    expressions: list[pl.Expr] = []
    
    # Add unique_id first
    expressions.append(pl.col("unique_id"))
    
    # For each predefined column, either use the existing column or create a null column
    for col_name in PREDEFINED_COLUMNS:
        if col_name in batch.columns:
            # Cast the column to its defined type if it exists in SCHEMA_OVERRIDES
            if col_name in SCHEMA_OVERRIDES:
                expressions.append(pl.col(col_name).cast(SCHEMA_OVERRIDES[col_name]))
            else:
                expressions.append(pl.col(col_name))
        else:
            # Create null column with correct type if defined
            null_type = SCHEMA_OVERRIDES.get(col_name, None)
            expressions.append(pl.lit(None, null_type).alias(col_name))
    
    # Return the DataFrame with all predefined columns
    return batch.select(expressions)

def process_file(input_csv: str):
    file_id = os.path.splitext(os.path.basename(input_csv))[0]
    
    # Create preprocessed-data directory if it doesn't exist
    preprocessed_dir = os.path.join(os.path.dirname(os.path.dirname(input_csv)), "preprocessed-data")
    os.makedirs(preprocessed_dir, exist_ok=True)
    
    # Set output path in preprocessed-data directory
    output_path = os.path.join(preprocessed_dir, f"{file_id}_preprocessed.parquet")
    
    # Skip if output file already exists
    if os.path.exists(output_path):
        print(f"\nSkipping {file_id} - already processed")
        return
    
    print(f"\nProcessing: {file_id}")
    print(f"Input: {input_csv}")
    print(f"Output: {output_path}")
    
    # Create a temporary directory for batch files
    temp_dir = tempfile.mkdtemp()
    batch_files: list[str] = []
    
    start_time = time.time()
    total_rows_processed = 0
    
    try:
        # Read and process the CSV in batches
        reader = pl.read_csv_batched(
            input_csv,
            batch_size=10000,
            schema_overrides=SCHEMA_OVERRIDES,
            null_values=["NA", ""]  # Treat NA and empty strings as null
        )
        
        batches = reader.next_batches(100)
        batch_counter = 0
        
        while batches:
            df_current_batches = pl.concat(batches)
            batch_row_count = len(df_current_batches)
            total_rows_processed += batch_row_count
            
            processed_batch = process_batch(df_current_batches, file_id)
            
            # Write each batch to a separate file
            batch_file = os.path.join(temp_dir, f"batch_{batch_counter}.parquet")
            processed_batch.write_parquet(batch_file)
            batch_files.append(batch_file)
            batch_counter += 1
            
            current_time = time.time()
            elapsed_time = current_time - start_time
            rows_per_second = total_rows_processed / elapsed_time if elapsed_time > 0 else 0
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] " + 
                  f"Processed {total_rows_processed:,} rows " + 
                  f"({rows_per_second:.2f} rows/sec). " + 
                  f"Elapsed: {timedelta(seconds=int(elapsed_time))}")
            
            batches = reader.next_batches(100)
        
        # After all batches are processed, combine them into final output
        print("Combining all batches into final output file...")
        if batch_files:
            # Read and combine all batch files
            final_df = pl.concat([pl.read_parquet(f) for f in batch_files])
            final_df.write_parquet(output_path)
            
            # Clean up temporary files
            for f in batch_files:
                os.remove(f)
            os.rmdir(temp_dir)
        
        print(f"Processing complete for {file_id}!")
        print(f"Finished! Processed {total_rows_processed:,} total rows in {timedelta(seconds=int(time.time() - start_time))}")
        print(f"Average speed: {(total_rows_processed / (time.time() - start_time)):.2f} rows/second")
        print(f"Output saved to: {output_path}\n")
        
    except Exception as e:
        print(f"Error processing {file_id}: {str(e)}")
        # Clean up temp files if they exist
        for f in batch_files:
            if os.path.exists(f):
                os.remove(f)
        if os.path.exists(temp_dir):
            os.rmdir(temp_dir)
        raise

def main():
    # Get all CSV files in raw-data directory
    raw_data_dir = os.path.join(os.path.dirname(__file__), "raw-data")
    csv_files = glob.glob(os.path.join(raw_data_dir, "*.csv"))
    
    print(f"Found {len(csv_files)} CSV files")
    
    # Count how many files need processing
    preprocessed_dir = os.path.join(os.path.dirname(raw_data_dir), "preprocessed-data")
    files_to_process = []
    for csv_file in csv_files:
        file_id = os.path.splitext(os.path.basename(csv_file))[0]
        output_path = os.path.join(preprocessed_dir, f"{file_id}_preprocessed.parquet")
        if not os.path.exists(output_path):
            files_to_process.append(csv_file)
    
    if not files_to_process:
        print("All files have already been processed!")
        return
        
    print(f"{len(files_to_process)} files need processing")
    
    # Process each file that needs it
    for i, csv_file in enumerate(files_to_process, 1):
        print(f"\nProcessing file {i} of {len(files_to_process)}")
        print("=" * 80)
        try:
            process_file(csv_file)
        except Exception as e:
            print(f"Failed to process {csv_file}: {str(e)}")
            print("Continuing with next file...")
            continue

if __name__ == "__main__":
    main()