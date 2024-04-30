# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common funcation/"

# COMMAND ----------

display(ingestion_date)

# COMMAND ----------

display(raw_folder_path)

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType, DateType, TimestampType

# COMMAND ----------

races_schema= StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("year", IntegerType(),True),
                                    StructField("round", IntegerType(), True),
                                    StructField("circuitId", IntegerType(), True),
                                    StructField("name",StringType(), True),
                                    StructField("date", DateType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("url", StringType(), True)])

# COMMAND ----------

races_df=spark.read.option("header", True).schema(races_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #step= transformation

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

races_dateandtimestamp_df=ingestion_date(races_df).withColumn("race_timestamp", to_timestamp(concat(col("date"),lit(" "),col("time")),"yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

display(races_dateandtimestamp_df)

# COMMAND ----------

races_renamed_df=races_dateandtimestamp_df.withColumnRenamed("raceId","race_id").withColumnRenamed("year","race_year").withColumnRenamed("circuitId","circuit_id")

# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

races_selected_df=races_renamed_df.select(col("race_id"),col("race_year"),col("round"),col("circuit_id"),col("name"),col("url"),col("ingestion_date"),col("race_timestamp")).withColumn("data_source", lit(v_data_source)).withColumn("file_date",lit(v_file_date))


# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC # write data to datalake as parquet

# COMMAND ----------

# races_selected_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{processed_folder_path}/races_parquet_file")

# COMMAND ----------

races_selected_df.write.mode("overwrite").format("parquet").saveAsTable("FR1_processed.race")

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------


