# Databricks notebook source
bronze_schema='bronze';
silver_schema='silver';
gold_schema='gold';
base_path="s3://fmcg-databricks-pipeline/raw_data";
landing_path="s3://fmcg-databricks-pipeline/raw_data/orders/landing";
processed_path="s3://fmcg-databricks-pipeline/raw_data/orders/processed";

# COMMAND ----------

