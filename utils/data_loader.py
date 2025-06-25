from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

class DataLoader:

  def __init__(self, configs: dict, spark: SparkSession):
    self.configs = configs
    self.spark = spark
    
  def load_csv(self, path: str) -> DataFrame:
    """
    Loads data from a csv file
    """
    if not path:
      raise ValueError(f'Path nor provided for csv file')
    return self.spark.read.csv(path)
  
  def load_parquet(self, path: str) -> DataFrame:
    """
    Loads data from a parquet file
    """
    if not path:
      raise ValueError(f'Path nor provided for parquet file')
    return self.spark.read.parquet(path)
  
  def load_sql(self, query: str) -> DataFrame:
    """
    Loads data from a query
    """
    if not query:
      raise ValueError(f'Query not provided for sql')
    return self.spark.sql(query)

  def load_dataframe(self) -> dict[str, DataFrame]: 
    """
    Handler that assigns the loader functions depending on the 
    configs metadata and the source types
    """
    dataframes = {}
    for tables in self.configs:
      table_name = tables.get('table_name')
      layer = tables.get('layer')
      path = tables.get('table_path')
      query = tables.get('query')
      source_type = tables.get('source_type')
      readers = {
        'parquet': lambda: self.load_parquet(path),
        'csv': lambda: self.load_csv(path),          
        'sql': lambda: self.load_sql(query),
      }
      if source_type not in readers:
          raise ValueError(f'Unsupported source type {source_type}')
      df = readers[source_type]()
      dataframes[table_name] = {
        'layer':layer,
        'table_name':table_name,
        'dataframe': df, 
        'metadata_path': tables.get('metadata_path')}
    return dataframes