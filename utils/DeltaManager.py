from delta.tables import *
from typing import Optional
from pyspark.sql.functions import *

class DeltaManager:
    def __init__(self, lakehouse_path: str, table_name: str, key_1: str
                 , key_2: str, date_column: str, spark: SparkSession
                 , layer: str , partition_cols: Optional[list[str]] = None):
        self.spark = spark
        self.layer = layer
        self.lakehouse_path = lakehouse_path
        self.table_name =  table_name
        self.partition_cols = partition_cols or []
        self.key_1 = key_1
        self.key_2 = key_2                
        self.date_column = date_column
        self.delta_table_path= f"{lakehouse_path}/{layer}/Tables/{table_name}"
        self.parquet_file_path= f"{lakehouse_path}/{layer}/Files/Incremental/{layer}.{table_name}/{table_name}.parquet"
        

    @property
    def delta_table(self) -> DeltaTable:
        if not hasattr(self, '_delta_table'):
            if self.check_delta_exists():
                self._delta_table = DeltaTable.ForPath(self.spark, self.delta_table_path)
            else:
                raise ValueError(f"Delta table at {self.delta_table_path} doesn't exist")
        return self._delta_table

    def read_parquet(self):
        return self.spark.read.parquet(self.parquet_file_path)
        
    def create_merge_expression(self) -> str:
        key_1, key_2 = self.key_1, self.key_2
        assert key_1 is not None, "Tables must have keys"
        if key_2 is None:
            merge_expression = f"t.{key_1} = s.{key_1}"
        else:
            merge_expression = f"t.{key_1} = s.{key_1} and t.{key_2} = s.{key_2}"
        return merge_expression
    
    def create_new_delta(self, df):
        df.write.format('delta') \
            .partitionBy(*self.partition_cols) \
            .save(self.delta_table_path)
    
    def merge_delta_table(self, df, merge_expression):        
        self.delta_table.alias('t') \
            .merge(df.alias('s'), merge_expression) \
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    def check_delta_exists(self) -> bool:
        return DeltaTable.IsDeltaTable(self.spark, self.delta_table_path)      

    def load(self) -> dict:
        df = self.read_parquet()
        df = df.withColumn('loaded_at', current_timestamp())
        merge_expression = self.create_merge_expression()
        try:
            if self.check_delta_exists():
                self.merge_delta_table(df, merge_expression)
            else:
                self.create_new_delta(df)               
        except Exception as e:
            print(e)
            return {"status": "error", "message": str(e)}
        print(f"Table {self.table_name} created successfully in { self.delta_table_path}")
        return self.get_load_metrics()
    

            
        

