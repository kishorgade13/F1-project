-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS formulaone_db
LOCATION "/";

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS hive_metastore.formulaone_db

-- COMMAND ----------

SELECT current_catalog()

-- COMMAND ----------

USE CATALOG hive_metastore

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

USE SCHEMA formulaone_db

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS formulaone_db.circuits;
CREATE TABLE IF NOT EXISTS formulaone_db.circuits(
circuitId INTEGER,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat FLOAT,
lng FLOAT,
alt INTEGER,
url STRING
)
USING CSV OPTIONS (path '/mnt/formula1adlsgen2source/raw/circuits.csv', header true)

-- COMMAND ----------

SELECT * FROM formulaone_db.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #create race table

-- COMMAND ----------

DROP TABLE IF EXISTS formulaone_db.races;
CREATE TABLE IF NOT EXISTS formulaone_db.races(
raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING
) USING CSV OPTIONS (path '/mnt/formula1adlsgen2source/raw/races.csv', header true)


-- COMMAND ----------

SELECT * FROM formulaone_db.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # create constructor table

-- COMMAND ----------

DROP TABLE IF EXISTS formulaone_db.constructor;
CREATE TABLE IF NOT EXISTS formulaone_db.constructor(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING JSON OPTIONS(path '/mnt/formula1adlsgen2source/raw/constructors.json', header true)

-- COMMAND ----------

SELECT * FROM formulaone_db.constructor

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #create driver table

-- COMMAND ----------

DROP TABLE IF EXISTS formulaone_db.driver;
CREATE TABLE IF NOT EXISTS formulaone_db.driver(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
) USING JSON OPTIONS (path '/mnt/formula1adlsgen2source/raw/drivers.json')

-- COMMAND ----------

SELECT * FROM formulaone_db.driver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #create result table

-- COMMAND ----------

DROP TABLE IF EXISTS formulaone_db.results;
CREATE TABLE IF NOT EXISTS formulaone_db.results(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points INT,
  laps INT,
  time STRING, 
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed FLOAT,
  statusId STRING
) USING JSON OPTIONS (path '/mnt/formula1adlsgen2source/raw/results.json', header true)


-- COMMAND ----------

SELECT * FROM formulaone_db.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #create pitstop table

-- COMMAND ----------

DROP TABLE IF EXISTS formulaone_db.pitstop;
CREATE TABLE IF NOT EXISTS formulaone_db.pitstops(
  raceId INT,
  driverId INT,
  stop STRING,
  lap INT,
  time STRING,
  duration STRING,
  milliseconds INT
) USING JSON OPTIONS(path '/mnt/formula1adlsgen2source/raw/pit_stops.json',multiLine true)

-- COMMAND ----------

SELECT * FROM formulaone_db.pitstops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # create tables for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # create lap times tables

-- COMMAND ----------

DROP TABLE IF EXISTS formulaone_db.laptimes;
CREATE TABLE IF NOT EXISTS formulaone_db.laptimes(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
) USING CSV OPTIONS(path '/mnt/formula1adlsgen2source/raw/lap_times')

-- COMMAND ----------

SELECT * FROM formulaone_db.laptimes

-- COMMAND ----------

SELECT count(1) FROM formulaone_db.laptimes;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #create qualifying table

-- COMMAND ----------

DROP TABLE IF EXISTS formulaone_db.qualifying;
CREATE TABLE IF NOT EXISTS formulaone_db.qualifying(
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
) USING JSON OPTIONS (path '/mnt/formula1adlsgen2source/raw/qualifying', multiLine true)

-- COMMAND ----------

SELECT * FROM formulaone_db.qualifying

-- COMMAND ----------

DESC EXTENDED formulaone_db.qualifying;

-- COMMAND ----------


