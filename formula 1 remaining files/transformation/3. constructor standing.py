# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

cosntructor_result_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(cosntructor_result_df)

# COMMAND ----------

from pyspark.sql.functions import count, sum, when, col

# COMMAND ----------

constructor_standing_df=cosntructor_result_df.groupBy("race_year","team").agg(sum("points").alias("total_points"),count(when(col("position")==1, True)).alias("wins"))

# COMMAND ----------

display(constructor_standing_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

constructor_window_spec=Window.partitionBy("team").orderBy(desc("total_points"), desc("wins"))
constructor_final_df=constructor_standing_df.withColumn("rank",rank().over(constructor_window_spec))


# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # write file to presentation layer

# COMMAND ----------

# constructor_final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")

# COMMAND ----------

 constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("FR_presentation.constructor_standings")

# COMMAND ----------


