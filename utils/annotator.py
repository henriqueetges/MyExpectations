import yaml
import datetime
import pyspark.sql.functions as F
from pyspark.sql.functions import concat, trim, lower
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.types as SType

class Annotator:
    def __init__(self, spark_session: SparkSession, df: SparkDataFrame, metadata_path: str):
        self.df = df
        self.spark_session = spark_session
        with open(metadata_path, 'r') as f:
          self.table_config = yaml.safe_load(f)        

    
    def get_column_tests(self) -> list[dict]:

      test_cols = [
      {
        'column': col.get('column_name'),
        'type': col.get('type'),
        'test_type': test.get('test_type', ''),
        'test_name': (
          test.get('test_type')
          +'.'+ col.get('column_name') 
          + ('.'+ test.get('test_name') if test.get('test_name') is not None else '')),
        'kwargs': test.get('kwargs', {})
      } 
      for col in self.table_config.get('columns', [])
      for test in col.get('tests', [])]
      return test_cols
        
    def Annotate_missing(self, **kwargs) -> SparkDataFrame: 
      column_name, test_name = kwargs.get('column', {}), kwargs.get('test_name', {})         
      self.df = (self.df.withColumn(f"{test_name}"
            , (F.col(column_name).isNull()) | (F.col(column_name).cast('string') == '0.0')))
      return self.df

    def Annotate_duplicated(self, **kwargs) -> SparkDataFrame:
      column_name, test_name = kwargs.get('column'), kwargs.get('test_name')
      window = Window.partitionBy(column_name).orderBy(F.lit(1))
      self.df = self.df.withColumn("row_number", F.row_number().over(window))
      self.df = self.df.withColumn(f"{test_name}", F.col("row_number") > 1)
      self.df = self.df.drop("row_number")
      return self.df

    def Annotate_outdated(self, **kwargs) -> SparkDataFrame:
      column_name, test_name = kwargs.get('column'), kwargs.get('test_name')         
      threshold = kwargs.get('kwargs', {}).get('threshold', {})
      today = datetime.date.today()
      self.df = (self.df.withColumn(f"{test_name}"
            , (F.col(column_name) < today - datetime.timedelta(days=threshold))))
      return self.df

    def Annotate_not_rules(self, **kwargs) -> SparkDataFrame:
      column_name, test_name = kwargs.get('column'), kwargs.get('test_name')         
      expression = kwargs.get('kwargs', {}).get('expression', {})
      self.df = self.df.withColumn(f"{test_name}",
              F.when(F.col(column_name).isNull(), F.lit(True)).otherwise(F.expr(expression))) 
      return self.df 

    def Annotate_not_in_list(self, **kwargs) -> SparkDataFrame:
      column_name, test_name = kwargs.get('column'), kwargs.get('test_name')         
      expected_values = kwargs.get('kwargs',{}).get('expected_values', [])
      self.df = (self.df.withColumn(f"{test_name}"
            , ~lower(trim(F.col(column_name))).isin([v.lower() for v in expected_values])))
      return self.df      

    def Annotate_pattern_inconsistency(self, **kwargs):
      column_name, pattern = kwargs.get('column'), kwargs.get('pattern')
      if column_name is None or pattern is None:
        raise ValueError('Unespecified column or pattern')
      self.df = self.df.withColumn(f'pattern_missmatch.{column_name}',
        F.when(~F.col(column_name).rlike(pattern), True).otherwise(False))
      return self.df

    
    def Annotate_type_inconsistency(self, **kwargs):
      column_name, expected_type = kwargs.get('column'), kwargs.get('type')
      if column_name is None or expected_type is None:
        raise ValueError('Unespecified column or column type')
      type_map = {
        'string': SType.StringType(),
        'integer': SType.IntegerType(),
        'float': SType.FloatType(),
        'boolean': SType.BooleanType()
      }
      actual_type = self.df.schema[column_name].dataType
      is_missmatch = actual_type != type_map.get(expected_type)
      self.df = self.df.withColumn(f'type_missmatch.{column_name}', F.lit(is_missmatch))
      return self.df

      
     

    # def Annotate_not_consistent_with(self, column_name: str, reference_df: SparkDataFrame,**kwargs)-> SparkDataFrame:      
    #   for col_info in kwargs['column_names']:
    #     column_name = col_info['name']
    #     reference_column = col_info['reference_column']
    #     mapped_table = col_info['mapped_table']
    #     common_key = col_info['common_key']
    #     self.df = self.df.alias('source')
    #     reference_df = self.spark_session.read.parquet(mapped_table, header=True, sep=';', inferSchema=True).select(common_key, reference_column).alias('reference')
    #     self.df = self.df.join(reference_df, on=self.df[f'source.{common_key}'] == reference_df[f'reference.{common_key}'], how="left")
    #     self.df = (self.df.withColumn(f"{column_name}_not_consistent_with"
    #       , F.col(f'source.{column_name}') != F.col(f'reference.{reference_column}')))
    #     self.df = self.df.drop(f'reference.{reference_column}')
    #   return self.df

    def Annotate(self) -> SparkDataFrame:
      expectation_funcs = {
        'is_missing': self.Annotate_missing,
        'duplicated': self.Annotate_duplicated,
        'not_timeliness': self.Annotate_outdated,
        'outside_of_rules': self.Annotate_not_rules,
        'not_in_list': self.Annotate_not_in_list,  
        'type_missmatch': self.Annotate_type_inconsistency,
        'pattern_missmatch':self.Annotate_pattern_inconsistency,
        #consistency': self.Annotate_not_consistent_with,      
     }
      tests = self.get_column_tests()
      for test_params in tests:
        if test_params.get('test_type') not in expectation_funcs:
          raise ValueError(f'Unsupported expectation type: {test_params.get("test_type")}')
        else:
            self.df = expectation_funcs[test_params.get('test_type')](**test_params)          
      return self.df


    

           
          



