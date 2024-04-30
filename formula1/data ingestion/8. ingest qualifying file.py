# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common funcation"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

qualifying_schema=StructType([StructField("qualifyId", IntegerType(), False), 
                            StructField("raceId", IntegerType(), True), 
                            StructField("driverId", IntegerType(), True), 
                            StructField("constructorId", IntegerType(), True), 
                            StructField("number", IntegerType(), True), 
                            StructField("position", IntegerType(), True), 
                            StructField("q1", StringType(), True), 
                            StructField("q2", StringType(), True), 
                            StructField("q3", StringType(), True),])

# COMMAND ----------

qualifying_df=spark.read.schema(qualifying_schema).option("multiline", True).json(f"{raw_folder_path}/qualifying/")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #transformation

# COMMAND ----------

qualifying_ingestiondate_df=ingestion_date(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

qualifying_final_df=qualifying_ingestiondate_df.withColumnRenamed("qualifyId","qualify_id").withColumnRenamed("driverId","driver_id").withColumnRenamed("raceId","race_id").withColumnRenamed("constructorId","constructor_id").withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # write to parquet file

# COMMAND ----------

# qualifying_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying_parquet_file")

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("FR1_processed.qualifying")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------


