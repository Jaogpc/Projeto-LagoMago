from pyspark.sql.functions import col

def table_exists(spark, database, table):
    tables_df = spark.sql(f"SHOW TABLES IN {database}")
    count = tables_df.filter((col("database") == database) & (col("tableName") == table)).count()
    return count == 1