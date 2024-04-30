# Databricks notebook source
# MAGIC %md 
# MAGIC # ingest circuit .csv file

# COMMAND ----------

# MAGIC %md 
# MAGIC # step1-- read the csv file using spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common funcation"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema= StructType(fields=[StructField("circuitId", IntegerType(), False),
                                    StructField("circuitRef", StringType(),True),
                                    StructField("name", StringType(), True),
                                    StructField("location", StringType(), True),
                                    StructField("country",StringType(), True),
                                    StructField("lat", DoubleType(), True),
                                    StructField("lng", DoubleType(), True),
                                    StructField("alt", IntegerType(),True),
                                    StructField("url", StringType(), True)])

# COMMAND ----------

circuits_df=spark.read.option("header", True).schema(circuits_schema).csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #step2 select the required columns

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

circuits_selected_df=circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # step3- rename the column as required 

# COMMAND ----------

circuits_renamed_df=circuits_selected_df.withColumnRenamed("circuitId","circuit_id").withColumnRenamed("circuitRef","circuit_ref").withColumnRenamed("lat","latitude").withColumnRenamed("lng","longitude").withColumnRenamed("alt","altitude").withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # step4-- add ingestion date to the dataframe

# COMMAND ----------

circuits_final_df=ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # step5- write data to datalake as parquet

# COMMAND ----------

# circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits_parquet_file")

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("FR1_processed.circuits")

# COMMAND ----------



# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

dbutils.notebook.exit("success")
