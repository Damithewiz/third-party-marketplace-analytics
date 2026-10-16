# Databricks notebook source
print("NEXT Marketplace Intelligence Platform")
print("Databricks environment is ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.next_third_party_landing;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.next_third_party_bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.next_third_party_silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.next_third_party_gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW SCHEMAS IN workspace LIKE 'next_third_party*';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS
# MAGIC workspace.next_third_party_landing.source_files;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW VOLUMES IN workspace.next_third_party_landing;