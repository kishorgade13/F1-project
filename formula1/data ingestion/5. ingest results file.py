# Databricks notebook source
spark.read.json("/mnt/formula1adlsgen2source/raw/2021-03-21/results.json").createOrReplaceTempView("results_cutover")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raceId,count(1) FROM results_cutover
# MAGIC GROUP BY raceId
# MAGIC ORDER BY raceId DESC

# COMMAND ----------

spark.read.json("/mnt/formula1adlsgen2source/raw/2021-03-28/results.json").createOrReplaceTempView("results_W2")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raceId,count(1) FROM results_W2
# MAGIC GROUP BY raceId
# MAGIC ORDER BY raceId DESC

# COMMAND ----------

spark.read.json("/mnt/formula1adlsgen2source/raw/2021-04-18/results.json").createOrReplaceTempView("results_W1")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raceId,count(1) FROM results_W1
# MAGIC GROUP BY raceId
# MAGIC ORDER BY raceId DESC

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common funcation"

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, time, FloatType

# COMMAND ----------

result_schema=StructType([StructField("resultId", IntegerType(), False),
                        StructField("raceId", IntegerType(), True), 
                        StructField("driverId", IntegerType(), True),
                        StructField("constructorId",IntegerType(), True), 
                        StructField("number",IntegerType(),True),
                        StructField("grid",IntegerType(), True), 
                        StructField("position",IntegerType(),True), 
                        StructField("positionText",StringType(),True), 
                        StructField("positionOrder",IntegerType(), True), 
                        StructField("points",IntegerType(),True),
                        StructField("laps",IntegerType(),True),
                        StructField("time",StringType(),True),
                        StructField("milliseconds",IntegerType(), True),
                        StructField("fastestLap",IntegerType(),True),
                        StructField("rank",IntegerType(),True),
                        StructField("fastestLapTime", StringType(),True), 
                        StructField("fastestLapSpeed", FloatType(), True), 
                        StructField("statusId", StringType(), True)])

# COMMAND ----------

results_df=spark.read.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC  %md
# MAGIC  # renaming column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------

results_ingestiondate_df=ingestion_date(results_df)

# COMMAND ----------

results_final_df=results_ingestiondate_df.withColumnRenamed("resultId","result_id").withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumnRenamed("constructorId","constructor_id").withColumnRenamed("positionText","position_text").withColumnRenamed("positionOrder","position_order").withColumnRenamed("fastestLap","fastest_lap").withColumnRenamed("fastestLapTime","fastest_lap_time").withColumnRenamed("fastestLapSpeed","fastest_lap_speed").drop(col("statusId")).withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # write data to parquet file

# COMMAND ----------

# results_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/result_parquet_file")

# COMMAND ----------

# MAGIC %sql
# MAGIC MSCK REPAIR TABLE FR1_processed.result

# COMMAND ----------

for race_id_list in results_final_df.select("race_id").distinct().collect():
    if (spark._jsparkSession.catalog().tableExists("FR1_processed.result")):
        spark.sql(f"ALTER TABLE FR1_processed.result DROP IF EXISTS PARTITION (race_id={race_id_list.race_id})")



# COMMAND ----------

results_final_df.write.mode("append").format("parquet").saveAsTable("FR1_processed.result")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM FR1_processed.result

# COMMAND ----------

dbutils.notebook.exit("sucess")

# COMMAND ----------


