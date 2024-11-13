# Databricks notebook source
# DBTITLE 1,Imports
import sys
sys.path.insert(0, "../lib/")
import utils
import ingestors
import os
import delta

# COMMAND ----------

# catalog = dbutils.widgets.get("catalog")
catalog = "hive_metastore"
# schema = dbutils.widgets.get("schema")
schema = "bronze"
# tablename = dbutils.widgets.get("tablename")
tablename = "titanic"

# COMMAND ----------

# DBTITLE 1,Criação de variáveis chave
account_name="projetodatalagomago"
account_key=os.getenv("BLOB_KEY")
container_name="raw"
src_url=f"wasbs://{container_name}@{account_name}.blob.core.windows.net"
conf_key=f"fs.azure.account.key.{account_name}.blob.core.windows.net"
mount_name=f"/mnt/project/"
path = "/mnt/project/"


# COMMAND ----------

# DBTITLE 1,Construção do mount
#mount já criado
#dbutils.fs.mount(source=src_url, mount_point=mount_name, extra_configs={conf_key:account_key})

# COMMAND ----------

# DBTITLE 1,Leitura do arquivo
df = spark.read.format("csv").option("header", "true").load(path)
(df.coalesce(1)
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(f"{schema}.{tablename}"))



# COMMAND ----------

# DBTITLE 1,Variável que guarda o schema
#schema = df.schema
#schema.json()

# COMMAND ----------

# DBTITLE 1,Verificação de mount
dbutils.fs.ls("./mnt")

# COMMAND ----------

# DBTITLE 1,Confirmação de leitura
df.show()

# COMMAND ----------

# DBTITLE 1,Stream
#não executável

#df_stream = (spark.readStream
                #.format("cloudFiles")
                #.option("cloudFiles.format", "csv")
                #.option("cloudFiles.schemaLocation", "/mnt/project/titanic/schema")
                #.option("cloudFiles.inferColumnTypes", "true")
                #.load("/mnt/project"))
#
#stream = (df_stream.writeStream
          #.option("checkpointLocation", "/mnt/project/titanic_checkpoint")
          #.foreachBatch(lambda df, batchID: upsert(df, bronze)))


# COMMAND ----------

# DBTITLE 1,Inicialização do stream
#não irá executar por usar uma base estática

#start = stream.start()

# COMMAND ----------

# DBTITLE 1,Ingestão Full Load
if not utils.table_exists(spark, "bronze", "titanic"):
    print("Tabela sendo criada!")

    ingest = ingestors.Ingestor(catalog = catalog, 
                      schema = schema, 
                      tablename = tablename, 
                      data_format= "csv")
    
    ingest.execute(path)
    
else:
    print("Tabela já existente, ignorando Full Load!")


# COMMAND ----------

# DBTITLE 1,Upsert
query = ''' 
            SELECT * FROM bronze.titanic
        '''
df_unique = spark.sql(query)

bronze = delta.DeltaTable.forName(spark, "bronze.titanic")

(bronze.alias("b")
    .merge(
        df_unique.alias("d"),
        "b.Passengerid = d.Passengerid"
    )
    .whenMatchedDelete(condition="d.2urvived = '1'")
    .whenMatchedUpdateAll(condition="d.2urvived = '0'")
    .whenNotMatchedInsertAll(condition="d.2urvived = '2'")
    .execute()
)

# COMMAND ----------

# DBTITLE 1,Visualização da tabela
# MAGIC %sql
# MAGIC SELECT * FROM bronze.titanic

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE titanic

# COMMAND ----------


