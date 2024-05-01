# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed("location","circuit_location")

# COMMAND ----------

race_df = spark.read.format("parquet").load(f"{processed_folder_path}/race").withColumnRenamed("name","race_name").withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

constructor_df=spark.read.format("parquet").load(f"{processed_folder_path}/constructor").withColumnRenamed("name","team")

# COMMAND ----------

driver_df=spark.read.format("parquet").load(f"{processed_folder_path}/driver").withColumnRenamed("number","driver_number").withColumnRenamed("name","driver_name").withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

result_df=spark.read.format("parquet").load(f"{processed_folder_path}/result").withColumnRenamed("time","race_time")

# COMMAND ----------

# MAGIC %md
# MAGIC # join the dataframes

# COMMAND ----------

race_circuit_df=circuits_df.join(race_df,circuits_df.circuit_id==race_df.circuit_id,"inner").select(race_df.race_id,race_df.race_year, race_df.race_name, race_df.race_date, circuits_df.circuit_location )

# COMMAND ----------

final_df=result_df.join(race_circuit_df, result_df.race_id==race_circuit_df.race_id).join(driver_df, result_df.driver_id==driver_df.driver_id).join(constructor_df,result_df.constructor_id==constructor_df.constructor_id)

# COMMAND ----------

race_result_df=final_df.select("race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","position")

# COMMAND ----------

display(race_result_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

race_result_df.withColumn("created_date", current_timestamp())

# COMMAND ----------

display(race_result_df)

# COMMAND ----------

display(race_result_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(race_result_df.points))

# COMMAND ----------

# race_result_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_result_df.write.mode("overwrite").format("parquet").saveAsTable("FR_presentation.race_results")

# COMMAND ----------


