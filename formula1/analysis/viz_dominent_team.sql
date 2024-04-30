-- Databricks notebook source
-- MAGIC %python
-- MAGIC html=""

-- COMMAND ----------

SELECT team_name,count(1) AS total_races, sum(calculated_points) AS total_points, avg(calculated_points) AS avg_points, rank() OVER (ORDER BY avg(calculated_points) DESC )AS team_rank FROM fr_presentation.calculated_race_results
WHERE race_year BETWEEN 2001 AND 2020
GROUP BY team_name
HAVING count(1)>=50
ORDER BY avg_points DESC

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominent_team
AS
SELECT race_year,team_name,count(1) AS total_races, sum(calculated_points) AS total_points, avg(calculated_points) AS avg_points, rank() OVER (ORDER BY avg(calculated_points) DESC )AS team_rank FROM fr_presentation.calculated_race_results
GROUP BY team_name, race_year
ORDER BY race_year,avg_points DESC

-- COMMAND ----------

SELECT race_year,team_name,count(1) AS total_races, sum(calculated_points) AS total_points, avg(calculated_points) AS avg_points FROM fr_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_dominent_team WHERE team_rank <=10)
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------


