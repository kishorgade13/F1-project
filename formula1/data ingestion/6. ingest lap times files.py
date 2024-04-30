# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common funcation"

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType

# COMMAND ----------

laptimes_schema=StructType([StructField("raceId", IntegerType(), False),
                            StructField("driverId", IntegerType(), True), 
                            StructField("lap", IntegerType(), True), 
                            StructField("position", IntegerType(), True), 
                            StructField("time", StringType(), True), 
                            StructField("milliseconds", IntegerType(), True)])

# COMMAND ----------

laptimes_df=spark.read.schema(laptimes_schema).csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

display(laptimes_df)

# COMMAND ----------

laptimes_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # transformation

# COMMAND ----------

laptimes_ingestiondate_df=ingestion_date(laptimes_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

laptimes_final_df=laptimes_ingestiondate_df.withColumnRenamed("driverId","driver_id").withColumnRenamed("raceId","race_id").withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(laptimes_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # write file to parquet

# COMMAND ----------

# laptimes_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/laptimes_parquet_file")

# COMMAND ----------

laptimes_final_df.write.mode("overwrite").format("parquet").saveAsTable("FR1_processed.laptimes")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------


