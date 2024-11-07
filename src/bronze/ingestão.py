# Databricks notebook source
# DBTITLE 1,Imports
import os
import delta

import sys

sys.path.insert(0, "../lib/")

import utils

# COMMAND ----------

# DBTITLE 1,Criação de variáveis chave
account_name="projetodatalagomago"
account_key=os.getenv("BLOB_KEY")
container_name="raw"
src_url=f"wasbs://{container_name}@{account_name}.blob.core.windows.net"
conf_key=f"fs.azure.account.key.{account_name}.blob.core.windows.net"
mount_name=f"/mnt/project/"

# COMMAND ----------

# DBTITLE 1,Construção do mount
dbutils.fs.mount(source=src_url, mount_point=mount_name, extra_configs={conf_key:account_key})

# COMMAND ----------

# DBTITLE 1,Leitura do arquivo
df = spark.read.format("csv").load(f"/mnt/project/")
(df.coalesce(1)
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable("bronze.titanic"))

# COMMAND ----------

# DBTITLE 1,Variável que guarda o schema
schema = df.schema
schema.json()

# COMMAND ----------

# DBTITLE 1,Verificação de mount
dbutils.fs.ls("./mnt")

# COMMAND ----------

# DBTITLE 1,Confirmação de leitura
df.show()

# COMMAND ----------

# DBTITLE 1,Visualização da tabela completa
# MAGIC %sql
# MAGIC SELECT * FROM bronze.titanic

# COMMAND ----------

# DBTITLE 1,Upsert
query = ''' 
            SELECT * FROM bronze.titanic 
        '''

df_unique = spark.sql(query)

bronze = delta.DeltaTable.forName(spark, "bronze.titanic")

(bronze.alias("b")
    .merge(df_unique.alias("d"),
    "b._c0 = d._c0")
    .whenMatchedDelete(condition = "d._c27 = '1'")
    .whenMatchedUpdateAll(condition = "d._c27 ='0'")
    .whenNotMatchedInsertAll(condition = "d._c27 = '2'")
.execute()
)

# COMMAND ----------

# DBTITLE 1,Stream
df_stream = (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("cloudFiles.schemaLocation", "/mnt/project/titanic/schema")
                .option("cloudFiles.inferColumnTypes", "true")
                .load("/mnt/project"))

stream = (df_stream.writeStream
          .option("checkpointLocation", "/mnt/project/titanic_checkpoint")
          .foreachBatch(lambda df, batchID: upsert(df, bronze)))



# COMMAND ----------

# DBTITLE 1,Inicialização do stream
start = stream.start()

#não irá executar por usar uma base estática

# COMMAND ----------

# DBTITLE 1,Visualização pós merge
# MAGIC %sql
# MAGIC SELECT * FROM bronze.titanic

# COMMAND ----------

# DBTITLE 1,Função de criação de tabela
if not utils.table_exists(spark, "bronze", "titanic"):
    df = spark.read.format("csv").load("/mnt/project/")
    (df.coalesce(1)
       .write
       .format("delta")
       .mode("overwrite")
       .saveAsTable("bronze.titanic"))
    
else:
    print("Tabela já existente, ignorando Full Load")
