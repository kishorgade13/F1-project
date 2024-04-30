# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/raeces_parquet_file")

# COMMAND ----------

display(races_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_filtered_df=races_df.filter(col("race_year")==2009)

# COMMAND ----------

display(races_filtered_df)

# COMMAND ----------

race_final_df=races_filtered_df.filter((col("race_year")==2009) & (col("round")>6))

# COMMAND ----------

display(race_final_df)

# COMMAND ----------


