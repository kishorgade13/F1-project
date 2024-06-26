# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common funcation"

# COMMAND ----------

 constructor_schema="constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df=spark.read.schema(constructor_schema).json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # transformation-- drop the column

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

constructor_dropped_df=constructor_df.drop(col("url"))

# COMMAND ----------

display(constructor_dropped_df)

# COMMAND ----------

constructor_ingestiondate_df=ingestion_date(constructor_dropped_df)

# COMMAND ----------

constructor_final_df=constructor_ingestiondate_df.withColumnRenamed("constructorId","constructor_id").withColumnRenamed("constructorRef","constructor_ref").withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # write the costructor parquet file

# COMMAND ----------

# constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructor_parquet_file")

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("FR1_processed.constructor")

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------


