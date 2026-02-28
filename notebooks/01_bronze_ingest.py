# Databricks notebook source
from pyspark.sql.functions import current_timestamp

spark.sql("USE CATALOG housing")

raw_base = "/Volumes/housing_landing/raw"
checkpoint_base = "/Volumes/housing_landing/checkpoints"

datasets = ["airbnb", "rentals", "amsterdam_areas", "post_codes"]

for ds in datasets:
    (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaLocation", f"{checkpoint_base}/{ds}_schema")
            .load(f"{raw_base}/{ds}/")
            .withColumn("ingested_at", current_timestamp())
            .writeStream
            .format("delta")
            .option("checkpointLocation", f"{checkpoint_base}/{ds}_checkpoint")
            .trigger(availableNow=True)
            .toTable(f"housing.bronze.{ds}_raw")
    )

print("All 4 bronze tables ingested using Auto Loader.")