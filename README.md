# traffic-stop-analysis

# Goal:

1. Attempt to determine if there is evidence of discrimination based on sex or race
2. Compare texas (generally conservative) vs california (generally liberal) traffic stop data to see if there are any general trend differences between the two.

# Data cleaning and exploration

### Quality issues

- 18 distinct csv files with different subsets of a large number of possible columns
  - Solution: convert all csv files to parquet, adding a unique identifier to each row and adding missing columns with null values. It was also necessary to expliclty type the the null columns correctly so duckdb didn't freak out.
- Some rows contain pipe-delimited values for IDs
  - Solution: it turned out that this was the result of deduplication in the original raw data. treating them as a strong solved the parsing issues.
- N/A was used as a null value in some cases
  - Solution: add N/A to the list of values to treat as null
- some columns appear to be numbers for the first several thousands rows and then eventually contain a letter
  - Solution: add these columsn as strings in the schema overrides
- 

## Inital data exploration

> The below is ouput from running `analyze_data_quality.py`

Columns in the data:
- unique_id
- raw_row_number
- date
- time
- location
- lat
- lng
- geocode_source
- county_name
- neighborhood
- beat
- district
- subdistrict
- division
- subdivision
- police_grid_number
- precinct
- region
- reporing_area
- sector
- subsector
- substation
- service_area
- zone
- subject_age
- subject_race
- subject_sex
- officer_id_hash
- officer_age
- officer_race
- officer_sex
- officer_years_of_service
- officer_assignment
- department_id
- department_name
- unit
- type
- disposition
- violation
- arrest_made
- citation_issued
- warning_issued
- outcome
- contraband_found
- contraband_drugs
- contraband_weapons
- contraband_other
- frisk_performed
- search_conducted
- search_person
- search_vehicle
- search_basis
- reason_for_arrest
- reason_for_frisk
- reason_for_search
- reason_for_stop
- speed
- posted_speed
- vehicle_color
- vehicle_make
- vehicle_model
- vehicle_type
- vehicle_registration_state
- vehicle_year
- use_of_force_description
- use_of_force_reason
- notes

Data Quality Analysis
================================================================================
Total rows across all files: 71,285,775
Rows with valid race data: 66,496,191
Percentage of rows with valid race data: 93.3%

Race Distribution:
--------------------------------------------------------------------------------
Race                 Count           Percentage
--------------------------------------------------------------------------------
white                33,168,051      46.5%
hispanic             22,358,509      31.4%
black                7,869,602       11.0%
asian/pacific islander 3,100,029       4.3%
other                2,967,121       4.2%
NULL                 1,144,868       1.6%
unknown              677,595         1.0%