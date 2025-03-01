# traffic-stop-analysis

# Goal:

1. Attempt to determine if there is evidence of discrimination based on sex or race
2. Compare texas (generally conservative) vs california (generally liberal) traffic stop data to see if there are any general trend differences between the two.

# Data cleaning and exploration

### Quality issues

- 18 distinct csv files with different subsets of a large number of possible columns
  - Solution: convert all csv files to parquet, adding a unique identifier to each row and adding missing columns with null values
- Some rows contain pipe-delimited values for IDs
  - Solution: it turned out that this was the result of deduplication in the original raw data. treating them as a strong solved the parsing issues.
- N/A was used as a null value in some cases
  - Solution: add N/A to the list of values to treat as null
- some columns appear to be numbers for the first several thousands rows and then eventually contain a letter
  - Solution: add these columsn as strings in the schema overrides
