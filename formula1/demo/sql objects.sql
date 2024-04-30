-- Databricks notebook source
-- MAGIC %md
-- MAGIC # create database
-- MAGIC

-- COMMAND ----------

CREATE DATABASE demo;

-- COMMAND ----------


CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

DESC DATABASE demo;

-- COMMAND ----------

DESC DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # create managed tables uaing python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df.write.format("delta").saveAsTable("demo_race_result_python")

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED
demo_race_result_python

-- COMMAND ----------

SELECT * FROM demo_race_result_python
WHERE race_year=2020 ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #create external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df.write.format("parquet").option("path", f"abfss://{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_result_ext_py")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # views on tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #1. create temp view

-- COMMAND ----------



-- COMMAND ----------

CREATE TEMP VIEW v_race_results AS SELECT * FROM demo_race_result_python


-- COMMAND ----------

SELECT * FROM v_race_results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # create the global temp view

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results AS SELECT * FROM demo_race_result_python

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # create permanenet view

-- COMMAND ----------

CREATE OR REPLACE VIEW pv_race_results
AS SELECT * FROM demo_race_result_python

-- COMMAND ----------

SELECT * FROM pv_race_results

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------


