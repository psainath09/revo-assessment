-- Databricks notebook source

CREATE CATALOG IF NOT EXISTS housing;

-- COMMAND ----------
USE CATALOG housing;

-- COMMAND ----------

-- Create a volume for raw files
CREATE VOLUME IF NOT EXISTS housing_landing
COMMENT 'Volume for raw Airbnb and rentals data';

-- COMMAND ----------
CREATE SCHEMA IF NOT EXISTS bronze
-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS silver
-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS gold

