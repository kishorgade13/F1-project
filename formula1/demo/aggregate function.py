# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

demo_df=spark.read.parquet(f"{presentation_folder_path}/race_results").filter("race_year==2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum, avg

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.filter("driver_name='Lewis Hamilton'").select(sum("points"), countDistinct("race_name")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # groupBy aggregation

# COMMAND ----------

demo_df.groupBy("driver_name").sum("points").show()

# COMMAND ----------

demo_df.groupBy("race_name").count().show()

# COMMAND ----------

demo_df.groupBy("team").max("points").show()

# COMMAND ----------

demo_df.groupBy("driver_name").agg(sum("points"), countDistinct("race_name")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # window funcation

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

driverRankSpec=Window.partitionBy("race_year").orderBy(desc("points"))
demo_df.withColumn("rank", rank().over(driverRankSpec)).show()

# COMMAND ----------


