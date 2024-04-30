-- Databricks notebook source
DROP DATABASE IF EXISTS F1_db CASCADE;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.mounts())

-- COMMAND ----------

DESC DATABASE EXTENDED F1_db

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS F1_db.circuits(
raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING
) USING CSV OPTIONS (path '/mnt/formula1adlsgen2source/raw/circuits.csv', header true)

-- COMMAND ----------


