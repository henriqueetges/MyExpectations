import yaml
import logging
from pyspark.sql import DataFrame as SparkDataFrame
from utils.annotator.annotator import Annotator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AnnotationManager:
    def __init__(self, spark_session, config_path):
        self.spark = spark_session
        self.config_path = config_path
        with open(self.config_path, 'r') as f:
            self.config = yaml.safe_load(f)
    
    def run_annotations(self): 
      results = {}       
      for table_config in self.config:
        table_name = table_config.get('table_name')
        layer = table_config.get('layer')
        logging.info(f'Processing table: {layer}.{table_name}')
        try:
          df = self.load_dataframe()
          annotator = Annotator(self.spark, df, table_config)
          annotated_df = annotator.Annotate()
          results[f'{layer}.{table_name}'] = annotated_df
        except Exception as e:          
          logging.error(f'Failed to annotate: {layer}.{table_name}')
          print(e)
      return results
    
    def load_dataframe(self) -> SparkDataFrame:
        path = 'path/to/your/data.csv'  # Replace with your actual path or with the path from the config
        return self.spark.read.csv(path, header = True, sep=';', inferSchema = True)