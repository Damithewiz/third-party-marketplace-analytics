# Databricks notebook source
# MAGIC %md
# MAGIC Installing kaggle package

# COMMAND ----------

# MAGIC %pip install --quiet kaggle

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC Configuring kaggle credentials

# COMMAND ----------

import os
import json

KAGGLE_USERNAME = "akinluadamilola"
KAGGLE_KEY = "********************************"

os.environ["KAGGLE_USERNAME"] = KAGGLE_USERNAME
os.environ["KAGGLE_KEY"] = KAGGLE_KEY

print("Kaggle credentials configured.")

# COMMAND ----------

import kaggle

print("Kaggle package installed successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC Kaggle API Authentication

# COMMAND ----------

from kaggle.api.kaggle_api_extended import KaggleApi

api = KaggleApi()
api.authenticate()

print("Kaggle authentication successful")

# COMMAND ----------

# MAGIC %md
# MAGIC Loading o list dataset files into landing schema via kaggle

# COMMAND ----------

from pathlib import Path

landing_path = Path(
    "/Volumes/workspace/next_third_party_landing/source_files/olist"
)

landing_path.mkdir(parents=True, exist_ok=True)

api.dataset_download_files(
    dataset="olistbr/brazilian-ecommerce",
    path=str(landing_path),
    unzip=True
)

print(f"Dataset downloaded to: {landing_path}")

# COMMAND ----------

files = sorted([file.name for file in landing_path.iterdir()])

for file in files:
    print(file)
