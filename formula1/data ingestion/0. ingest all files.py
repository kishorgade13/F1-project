# Databricks notebook source
v_result=dbutils.notebook.run("1. ingest curcuits file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("2. ingest race file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("4. ingest driver file",0,{"":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("5. ingest results file",0,{"":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("6. ingest lap times files",0,{"":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("7. ingest pitstops file",0,{"":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("8. ingest qualifying file",0,{"":"Ergast API"})

# COMMAND ----------

v_result
