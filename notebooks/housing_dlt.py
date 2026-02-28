# housing_dlt.py
import dlt
from pyspark.sql.functions import current_timestamp, col, regexp_replace, upper

# ---------- BRONZE LAYER ----------

landing_path = "/Volumes/housing_landing/raw"

datasets = ["airbnb", "rentals", "amsterdam_areas", "post_codes"]

for ds in datasets:

    @dlt.table(
        name=f"housing.bronze.{ds}_raw",
        comment=f"Bronze table for {ds} data"
    )
    @dlt.expect_or_drop("valid_id", "id IS NOT NULL")  # optional constraint
    def bronze_table(ds=ds):
        return (
            spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", True)
                .option("cloudFiles.schemaLocation", f"/Volumes/housing_landing/checkpoints/{ds}_schema")
                .load(f"{landing_path}/{ds}/")
                .withColumn("ingested_at", current_timestamp())
        )

# ---------- SILVER LAYER ----------

@dlt.table(
    name="housing.silver.airbnb_clean",
    comment="Cleaned Airbnb data"
)
def silver_airbnb():
    df = dlt.read("housing.bronze.airbnb_raw")
    return (
        df.dropDuplicates(["id"])
          .withColumn("price", regexp_replace("price", "[^0-9.]", "").cast("double"))
          .withColumn("postcode", upper(regexp_replace("postcode", " ", "")))
          .filter("price > 0 AND postcode IS NOT NULL")
    )

@dlt.table(
    name="housing.silver.rentals_clean",
    comment="Cleaned Rentals data"
)
def silver_rentals():
    df = dlt.read("housing.bronze.rentals_raw")
    return (
        df.dropDuplicates(["id"])
          .withColumn("rent_price", regexp_replace("rent_price", "[^0-9.]", "").cast("double"))
          .withColumn("postcode", upper(regexp_replace("postcode", " ", "")))
          .filter("rent_price > 0")
    )

@dlt.table(
    name="housing.silver.amsterdam_areas_clean",
    comment="Amsterdam areas"
)
def silver_areas():
    return dlt.read("housing.bronze.amsterdam_areas_raw")

@dlt.table(
    name="housing.silver.post_codes_clean",
    comment="Post codes"
)
def silver_postcodes():
    return dlt.read("housing.bronze.post_codes_raw")

# ---------- GOLD LAYER ----------

@dlt.table(
    name="housing.gold.postcode_metrics",
    comment="Aggregated metrics per postcode"
)
def gold_postcode():
    airbnb = dlt.read("housing.silver.airbnb_clean")
    rentals = dlt.read("housing.silver.rentals_clean")
    return (
        airbnb.groupBy("postcode")
              .agg(
                  {"price": "avg", "*": "count"}
              )
              .withColumnRenamed("avg(price)", "avg_airbnb_price")
              .withColumnRenamed("count(1)", "total_airbnb_listings")
              .join(
                  rentals.groupBy("postcode").agg({"rent_price": "avg"})
                          .withColumnRenamed("avg(rent_price)", "avg_rent_price"),
                  "postcode",
                  "left"
              )
              .withColumn("rent_to_airbnb_ratio", col("avg_rent_price") / col("avg_airbnb_price"))
    )

@dlt.table(
    name="housing.gold.area_metrics",
    comment="Aggregated metrics per area"
)
def gold_area():
    airbnb = dlt.read("housing.silver.airbnb_clean")
    areas = dlt.read("housing.silver.amsterdam_areas_clean")
    return (
        airbnb.join(areas, airbnb.postcode == areas.postcode, "left")
              .groupBy("area_name")
              .agg(
                  {"*": "count", "price": "avg"}
              )
              .withColumnRenamed("count(1)", "total_airbnb_listings")
              .withColumnRenamed("avg(price)", "avg_airbnb_price")
    )