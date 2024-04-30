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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

name_schema=StructType([StructField("forename",StringType(),True),StructField("surname", StringType(), True)])

# COMMAND ----------

driver_schema = StructType([StructField("driverId", IntegerType (), False),
                            StructField("driverRef", StringType(), True),
                            StructField("number", IntegerType(), True),
                              StructField("code", StringType(), True),
                              StructField("name", name_schema), 
                              StructField("dob", DateType(),True), 
                              StructField("nationality",StringType(), True),
                               StructField("url", StringType(),True)])

# COMMAND ----------

driver_df=spark.read.schema(driver_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC # transformation
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,concat,lit,col

# COMMAND ----------

driver_dateingestion_df=ingestion_date(driver_df)

# COMMAND ----------

driver_final_df=driver_dateingestion_df.withColumnRenamed("driverId","driver_id").withColumnRenamed("driverRef","driver_ref").withColumn("name",concat(col("name.forename"),lit(" "), col("name.surname"))).drop(col("url")).withColumn("data_source", lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(driver_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # write data to parquet file

# COMMAND ----------

# driver_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/driver_parquet_file")

# COMMAND ----------

driver_final_df.write.mode("overwrite").format("parquet").saveAsTable("FR1_processed.driver")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------


