# Databricks notebook source
from pyspark.sql import functions as F

spark.sql("USE CATALOG housing")

# Gold 1: Postcode metrics
airbnb = spark.table("housing.silver.airbnb_clean")
rentals = spark.table("housing.silver.rentals_clean")

gold_postcode = (
    airbnb.groupBy("postcode")
         .agg(F.avg("price").alias("avg_airbnb_price"),
              F.count("*").alias("total_airbnb_listings"))
         .join(
             rentals.groupBy("postcode").agg(F.avg("rent_price").alias("avg_rent_price")),
             "postcode",
             "left"
         )
         .withColumn("rent_to_airbnb_ratio", F.col("avg_rent_price") / F.col("avg_airbnb_price"))
)

gold_postcode.write.format("delta").mode("overwrite").saveAsTable("housing.gold.postcode_metrics")

# Gold 2: Area metrics
areas = spark.table("housing.silver.amsterdam_areas_clean")

gold_area = (
    airbnb.join(areas, airbnb.postcode == areas.postcode, "left")
           .groupBy("area_name")
           .agg(F.count("*").alias("total_airbnb_listings"),
                F.avg("price").alias("avg_airbnb_price"))
)
gold_area.write.format("delta").mode("overwrite").saveAsTable("housing.gold.area_metrics")

print("2 gold tables created.")