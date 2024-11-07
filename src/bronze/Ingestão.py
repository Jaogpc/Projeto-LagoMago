# Databricks notebook source
# DBTITLE 1,Imports
import os
import delta
from pyspark.sql.functions import col

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
df = spark.read.format("csv").load("/mnt/project/")
(df.coalesce(1)
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable("bronze.titanic"))

# COMMAND ----------

# DBTITLE 1,Verificação
dbutils.fs.ls("./mnt")

# COMMAND ----------

# DBTITLE 1,Confirmação
df.show()

# COMMAND ----------

# DBTITLE 1,Visualização
# MAGIC %sql
# MAGIC SELECT * FROM bronze.titanic

# COMMAND ----------

# DBTITLE 1,Visualização
query = ''' 
            SELECT * FROM bronze.titanic 
        '''

df_unique = spark.sql(query)
df_unique.display()

# COMMAND ----------

# DBTITLE 1,Criação da tabela delta
bronze = delta.DeltaTable.forName(spark, "bronze.titanic")
bronze

# COMMAND ----------

# DBTITLE 1,Merge entre as tabelas delta e original
#UPSERT
(bronze.alias("b")
    .merge(df_unique.alias("d"),
    "b._c0 = d._c0")
    .whenMatchedDelete(condition = "d._c27 = '1'")
    .whenMatchedUpdateAll(condition = "d._c27 ='0'")
    .whenNotMatchedInsertAll(condition = "d._c27 = '2'")
.execute()
)

# COMMAND ----------

# DBTITLE 1,Visualização pós merge
# MAGIC %sql
# MAGIC SELECT * FROM bronze.titanic

# COMMAND ----------

# DBTITLE 1,Contagem de tabelas
def table_exists(database, table):
    tables_df = spark.sql(f"SHOW TABLES IN {database}")
    count = tables_df.filter((col("database") == database) & (col("tableName") == table)).count()
    return count == 1



# COMMAND ----------

# DBTITLE 1,Função de criação de tabela
if not table_exists("bronze", "titanic"):
    df = spark.read.format("csv").load("/mnt/project/")
    (df.coalesce(1)
       .write
       .format("delta")
       .mode("overwrite")
       .saveAsTable("bronze.titanic"))
    
else:
    print("Tabela já existente, ignorando Full Load")

# COMMAND ----------


