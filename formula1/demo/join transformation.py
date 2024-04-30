# Databricks notebook source
# inner joins

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_folder_path}/circuits_parquet_file").withColumnRenamed("name","circuits_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/races_parquet_file").withColumnRenamed("name","race_name")

# COMMAND ----------

display(races_df)

# COMMAND ----------



# COMMAND ----------

join_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id)

# COMMAND ----------

display(join_df)

# COMMAND ----------

selected_join_df=join_df.select(circuits_df.circuits_name, circuits_df.location,circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(selected_join_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC # outer join
# MAGIC

# COMMAND ----------

# 1. left or left outer

# COMMAND ----------

left_outer_joindf=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"leftouter").select(circuits_df.circuits_name,circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(left_outer_joindf)

# COMMAND ----------

# semi joins--> similar to inner joins fetch common record from both table, but in semi we get column from left table only, it display column from left data frame only none of the column is displayed from right data frame 

# COMMAND ----------

# anti join-->
