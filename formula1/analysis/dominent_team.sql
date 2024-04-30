-- Databricks notebook source
SELECT * FROM fr_presentation.calculated_race_results

-- COMMAND ----------

SELECT team_name, count(1), Sum(calculated_points) AS total_points, avg(calculated_points) AS avg_points FROM fr_presentation.calculated_race_results
GROUP BY team_name
HAVING count(1)>=50
ORDER BY avg_points DESC


-- COMMAND ----------


