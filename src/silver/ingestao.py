# Databricks notebook source
def import_query(path):
  with open(path, 'r') as open_file:
    return open_file.read()
  
tablename = "passageiro"
query = import_query(f"{tablename}.sql")

(spark.sql(query)
      .write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(f"silver.{tablename}"))

# COMMAND ----------



# COMMAND ----------


