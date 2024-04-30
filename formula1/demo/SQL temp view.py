# Databricks notebook source
# MAGIC %md
# MAGIC # access dataframe using SQL 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #1. create temp view on dataframes
# MAGIC #2. access the view from SQL cell
# MAGIC #3. access the view from python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_result_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_result_df.createTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_race_results
# MAGIC WHERE race_year==2020

# COMMAND ----------

# MAGIC %md
# MAGIC # we can use sql in python or scala

# COMMAND ----------

race_result2020_df=spark.sql("SELECT * FROM v_race_results WHERE race_year==2020")

# COMMAND ----------

display(race_result2020_df)

# COMMAND ----------

p_race_year="2020"

# COMMAND ----------

race_result2020_df=spark.sql(f"SELECT * FROM v_race_results WHERE race_year=={p_race_year}")

# COMMAND ----------

display(race_result2020_df)

# COMMAND ----------

# the local view will be available only for this notebook, if we try to access it from another notebook we won't be able to access

# COMMAND ----------

# MAGIC %md
# MAGIC # Global temp view
# MAGIC # 1. Create global temp views on data frames
# MAGIC # 2. acess the view from SQL cell
# MAGIC # 3. Acess the view from python cell
# MAGIC # 4. Acess the view from another notebook

# COMMAND ----------

race_result_df.createOrReplaceGlobalTempView("gv_global_view")

# COMMAND ----------

# MAGIC %md
# MAGIC # show temp view table

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC # show global temp view

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from global_temp.gv_global_view

# COMMAND ----------

# MAGIC %md 
# MAGIC # when we want to access the global temp view then we have to use global_temp.view_name

# COMMAND ----------

display(spark.sql("SELECT * FROM global_temp.gv_global_view"))

# COMMAND ----------

# MAGIC %md 
# MAGIC # we can access the globa view from one or many notebook
