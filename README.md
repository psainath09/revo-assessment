# Rent vs Airbnb – Databricks Lakehouse Implementation

## Architecture
Medallion Architecture (Bronze → Silver → Gold) implemented fully inside Databricks using:

- Unity Catalog
- Delta Lake
- Z-Ordering
- Table Constraints
- Workflows
- Databricks Asset Bundles

## Data Location
Data is stored in Unity Catalog Volume:
 /Volumes/housing/raw/

## Execution
1. Upload data to Volume
2. Deploy bundle
3. Run job

## Output
Final table:
 housing.gold.postcode_metrics