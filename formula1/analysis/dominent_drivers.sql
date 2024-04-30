-- Databricks notebook source
SELECT * FROM fr_presentation.calculated_race_results

-- COMMAND ----------

 
SELECT driver_name,count(1) AS total_races, sum(calculated_points) AS total_points, avg(calculated_points) AS avg_points FROM fr_presentation.calculated_race_results
WHERE race_year BETWEEN 2001 AND 2020
GROUP BY driver_name
HAVING count(1)>=50
ORDER BY avg_points DESC

-- COMMAND ----------

  <
