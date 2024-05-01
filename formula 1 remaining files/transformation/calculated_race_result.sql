-- Databricks notebook source
use fr1_processed;

-- COMMAND ----------

CREATE TABLE fr_presentation.calculated_race_results
USING parquet
AS
SELECT race.race_year,constructor.name AS team_name,driver.name AS driver_name,result.position, result.points, 11-result.position AS calculated_points
FROM result 
JOIN driver ON (result.driver_id==driver.driver_id)
JOIN constructor ON (result.constructor_id==constructor.constructor_id)
JOIN race ON (result.race_id==race.race_id)
WHERE result.position<=10

-- COMMAND ----------

SELECT * FROM fr_presentation.calculated_race_results

-- COMMAND ----------


