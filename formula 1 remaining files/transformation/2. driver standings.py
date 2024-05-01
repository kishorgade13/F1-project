# Databricks notebook source
# MAGIC %md
# MAGIC # produce driver standing

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

raceresult_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(raceresult_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count

# COMMAND ----------

driver_standing_df=raceresult_df.groupBy("race_year","driver_name","driver_nationality","team").agg(sum("points").alias("total_points"),count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(driver_standing_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

# COMMAND ----------

rankwindow_spec=Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df=driver_standing_df.withColumn("rank", rank().over(rankwindow_spec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # write file to presentation layer

# COMMAND ----------

#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("FR_presentation.driver_standings")
