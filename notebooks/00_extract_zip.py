# Databricks notebook source

import zipfile, os

landing_dir = "/Volumes/housing_landing"
raw_dir = f"{landing_dir}/raw"
os.makedirs(raw_dir, exist_ok=True)

# Airbnb & Rentals
for zf in ["airbnb.zip", "rentals.zip"]:
    with zipfile.ZipFile(f"{landing_dir}/{zf}", "r") as zip_ref:
        zip_ref.extractall(f"{raw_dir}/{zf.split('.')[0]}")

# Geo files
geo_dir = f"{landing_dir}/geo"
for zf in ["amsterdam_areas.zip", "post_codes.zip"]:
    with zipfile.ZipFile(f"{geo_dir}/{zf}", "r") as zip_ref:
        zip_ref.extractall(f"{raw_dir}/{zf.split('.')[0]}")

print("All ZIPs extracted to landing/raw/")