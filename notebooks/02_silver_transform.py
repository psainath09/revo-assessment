# Databricks notebook source
from pyspark.sql import functions as F

spark.sql("USE CATALOG housing")

# Airbnb
airbnb = spark.table("housing.bronze.airbnb_raw")
silver_airbnb = (
    airbnb.dropDuplicates(["id"])
           .withColumn("price", F.regexp_replace("price", "[^0-9.]", "").cast("double"))
           .withColumn("postcode", F.upper(F.regexp_replace("postcode", " ", "")))
           .filter("price > 0 AND postcode IS NOT NULL")
)
silver_airbnb.write.format("delta").mode("overwrite").saveAsTable("housing.silver.airbnb_clean")

# Rentals
rentals = spark.table("housing.bronze.rentals_raw")
silver_rentals = (
    rentals.dropDuplicates(["id"])
           .withColumn("rent_price", F.regexp_replace("rent_price", "[^0-9.]", "").cast("double"))
           .withColumn("postcode", F.upper(F.regexp_replace("postcode", " ", "")))
           .filter("rent_price > 0")
)
silver_rentals.write.format("delta").mode("overwrite").saveAsTable("housing.silver.rentals_clean")

# Amsterdam Areas
areas = spark.table("housing.bronze.amsterdam_areas_raw")
areas.write.format("delta").mode("overwrite").saveAsTable("housing.silver.amsterdam_areas_clean")

# Post Codes
postcodes = spark.table("housing.bronze.post_codes_raw")
postcodes.write.format("delta").mode("overwrite").saveAsTable("housing.silver.post_codes_clean")

print("All 4 silver tables created.")