-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Find dominant driver all the time

-- COMMAND ----------

SELECT driver_name, COUNT(*) AS total_races, SUM(calculated_points) AS total_points, AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING total_races >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Find dominant driver in a decade (2011-2020)

-- COMMAND ----------

SELECT driver_name, COUNT(*) AS total_races, SUM(calculated_points) AS total_points, AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY driver_name
HAVING total_races >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Find dominant driver in a decade (2001-2010)

-- COMMAND ----------

SELECT driver_name, COUNT(*) AS total_races, SUM(calculated_points) AS total_points, AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY driver_name
HAVING total_races >= 50
ORDER BY avg_points DESC