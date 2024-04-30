-- Databricks notebook source
USE formulaone_processed

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT * FROM formulaone_processed.circuits
WHERE country=="Australia"

-- COMMAND ----------

SELECT *,concat(location,'-',country) FROM formulaone_processed.circuits

-- COMMAND ----------

SELECT country,count(*)as location_count FROM formulaone_processed.circuits
GROUP BY country,location
HAVING count(*)>1

-- COMMAND ----------

SELECT * , rank() OVER (PARTITION BY country ORDER BY altitude DESC) FROM formulaone_processed.circuits

-- COMMAND ----------


