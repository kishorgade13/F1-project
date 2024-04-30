-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS FR_presentation
LOCATION '/mnt/formula1adlsgen2source/presentation';

-- COMMAND ----------

USE FR_presentation

-- COMMAND ----------

select current_database();

-- COMMAND ----------

DESC DATABASE EXTENDED FR_presentation

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

drop database FR_presentation cascade

-- COMMAND ----------


