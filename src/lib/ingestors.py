import delta
import utils

class Ingestor:

    def __init__(self, mount_name, schema, tablename, data_format):
        
        self.catalog = mount_name
        self.schemaname = schema
        self.tablename = tablename
        self.format = data_format
        self.set_schema()

    def set_schema(self, schema):
        self.data_schema = schema

    def load(self, path):
        df = (spark.read
                    .format(self.format)
                    .schema(self.data_schema)
                    .load(self.catalog))
        return df
        
    def save(self, df):
        (df.coalesce(1)
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(self.tablename))
        
    def execute(self, path):
        df = self.load(self.catalog)
        self.save(df)

class IngestorCDC(Ingestor):

    def __init__(self,catalog, shemaname, tablename, data_format):

        super().__init__(catalog, shemaname, tablename, data_format)
        self.set_deltatable()

    def set_deltatable(self):
        tablename = f"{self.catalog}.{self.tablename}"
        self.deltatable = delta.DeltaTable.forName(spark, tablename)
    
    #em tese entraria a parte de streaming agora, mas como comentado anteriormente, o streaming não funciona nesse dataset