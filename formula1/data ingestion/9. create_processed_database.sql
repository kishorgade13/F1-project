-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS FR1_processed
LOCATION '/mnt/formula1adlsgen2source/processed'

-- COMMAND ----------

use FR1_PROCESSED 

-- COMMAND ----------

DROP DATABASE f1_processed;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

desc database FR1_processed

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS hive_metastore.F1_processed

-- COMMAND ----------

SELECT current_catalog()

-- COMMAND ----------

USE CATALOG hive_metastore

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

USE database F1_processed

-- COMMAND ----------

DESC DATABASE F1_processed

-- COMMAND ----------


