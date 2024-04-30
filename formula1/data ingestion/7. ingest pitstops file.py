# Databricks notebook source
# MAGIC %md
# MAGIC # read the file
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common funcation"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

pitstop_schema=StructType([StructField("raceId", IntegerType(), False),
                            StructField("driverId", IntegerType(), True), 
                            StructField("stop", StringType(), True),
                            StructField("lap",IntegerType(), True), 
                            StructField("time", StringType(),True),
                            StructField("duration",StringType(),True), 
                            StructField("milliseconds", IntegerType(), True)])

# COMMAND ----------

pitstop_df=spark.read.schema(pitstop_schema).option("multiline",True).json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

display(pitstop_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # tranformation 

# COMMAND ----------

pitstop_ingestiondate_df=ingestion_date(pitstop_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

Pitstop_final_df=pitstop_ingestiondate_df.withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(Pitstop_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # write to parquet file

# COMMAND ----------

# Pitstop_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pitstop_parquet_file")

# COMMAND ----------

Pitstop_final_df.write.mode("overwrite").format("parquet").saveAsTable("FR1_processed.pitstop")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------


