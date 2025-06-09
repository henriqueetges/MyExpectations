import yaml
import logging
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from utils import Annotator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AnnotationManager:
    def __init__(self, spark_session, dfs):
        self.spark = spark_session
        self.dfs = dfs  
        
    def run_annotations(self): 
      results = {}       
      for table_name, table in self.dfs.items():
        layer = table.get('layer')
        metadata_path = table.get('metadata_path')
        df = table.get('dataframe')
        try:          
          annotator = Annotator(self.spark, df, metadata_path)
          annotated_df = annotator.Annotate()
          results[f'{layer}.{table_name}'] = annotated_df
        except Exception as e:          
          logging.error(f'Failed to annotate: {layer}.{table_name}')
          print(e)
      return results

    def get_completeness(self, df):
      tested_columns = [col for col in df.columns if col.startswith('is_missing.')]
      if not tested_columns:
          return df.withColumn('completeness', F.lit(1.0)) 
      return df.withColumn('completeness', 1-(
                           sum(F.col(f"`{c}`").cast('int')for c in tested_columns)
                           / len(tested_columns)))
    
    def get_consistency(self, df):
      tested_columns = [col for col in df.columns 
        if col.startswith('type_missmatch') 
        or col.startswith('inconsistent')]
      if not tested_columns:
        return df.withColumn('consistency', F.lit(1.0))
      return df.withColumn('consistency', 1 - (
                            sum(F.col(f"`{c}`").cast('int') for c in tested_columns)
                            / len(tested_columns)))

    def get_uniqueness(self, df):
      tested_columns = [col for col in df.columns  if col.startswith('duplicated.')]
      if not tested_columns:
          return df.withColumn('uniqueness', F.lit(1.0))

      return df.withColumn('uniqueness', 1-(
                           sum(F.col(f"`{c}`").cast('int')for c in tested_columns)
                           / len(tested_columns)))

    def get_accuracy(self, df):
      tested_columns = [
          col for col in
          df.columns  if col.startswith('not_in_list.')
          or col.startswith('outside_of_rules.')
          ]
      if not tested_columns:
          return df.withColumn('accuracy', F.lit(1.0)) 

      return df.withColumn('accuracy', 1-(
                           sum(F.col(f"`{c}`").cast('int')for c in tested_columns)
                           / len(tested_columns)))

    def get_timeliness(self, df):
      tested_columns = [col for col in df.columns  if col.startswith('is_outdated.')]
      if not tested_columns:
          return df.withColumn('timeliness', F.lit(1.0)) 

      return df.withColumn('timeliness', 1-(
                           sum(F.col(f"`{c}`").cast('int') for c in tested_columns)
                           / len(tested_columns)))
      
    def run_results(self):
      """THIS PROBABLY CAN BE REFACTORED INTO ITS OWN CLASS ALONG WITH
      THE INDIVIDUAL TESTS"""
      results = {}
      test_funcs = [
        self.get_accuracy,
        self.get_consistency,
        #self.get_timeliness, 
        self.get_uniqueness ,
        #self.get_currency, 
        self.get_completeness]
      
      annotations = self.run_annotations()
      for table, df in annotations.items():
        for func in test_funcs:
          df = func(df)
          results[f'{table}'] = df
      return results
    
    def calculate_quality(self):
      results = {}
      dimension_scores = self.run_results()
      for table, df in dimension_scores.items():
        df = df.withColumn('QScore'
          , F.round((
          (F.col('accuracy') 
          + F.col('consistency')
          + F.col('uniqueness') 
          + F.col('completeness')) / 4), 2))
        results[f'{table}'] = df
      return results
          
if __name__ == '__main__':
  pass