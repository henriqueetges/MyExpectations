import yaml
import datetime
import pyspark.sql.functions as F
from pyspark.sql.functions import trim, lower
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from collections import defaultdict

class Annotator:
    def __init__(self, spark_session: SparkSession,df: SparkDataFrame, table_config: dict):
        self.df = df
        self.spark_session = spark_session
        self.table_config = table_config   
    
    def get_column_tests(self) -> dict:       
      col_config = [col for col in self.table_config.get('columns')]  
      test_cols = defaultdict(list)
      for item in col_config:
        column = item['column_name']
        for test in item['tests']:
          test_name = test['name']
          kwargs = {k: v for k, v in test.items() if k != 'name'}
          entry = {'column': column, 'kwargs': kwargs.get('kwargs', {})}
          test_cols[test_name].append(entry)
      return dict(test_cols)
        
    def Annotate_missing(self, column_name, **kwargs) -> SparkDataFrame:          
      self.df = (self.df.withColumn(f"{column_name}_missing"
            , F.col(column_name).isNull()))
      return self.df

    def Annotate_duplicated(self, column_name, **kwargs) -> SparkDataFrame:
      window = Window.partitionBy(column_name).orderBy(F.lit(1))
      self.df = self.df.withColumn("row_number", F.row_number().over(window))
      self.df = self.df.withColumn(f"{column_name}_duplicated", F.col("row_number") > 1)
      self.df = self.df.drop("row_number")
      return self.df

    def Annotate_outdated(self, column_name,**kwargs) -> SparkDataFrame:
      threshold = kwargs.get('threshold')
      today = datetime.date.today()
      self.df = (self.df.withColumn(f"{column_name}_outdated"
            , (F.col(column_name) < today - datetime.timedelta(days=threshold))))
      return self.df

    def Annotate_not_rules(self, column_name,**kwargs) -> SparkDataFrame:
      expr = kwargs['expression']
      self.df = (self.df .withColumn(f"{column_name}_outside_of_sules"
              , F.expr(expr)))
      return self.df 

    def Annotate_not_in_list(self, column_name,**kwargs):
      expected_values = kwargs.get('expected_values', [])
      self.df = (self.df.withColumn(f"{column_name}_not_valid_attribute"
            , ~lower(trim(F.col(column_name))).isin([v.lower() for v in expected_values])))
      return self.df

    def inspect_kwargs(self, column_name, **kwargs):
      d = dict(kwargs)
      print(d)

    def Annotate_not_consistent_with(self, column_name: str, reference_df: SparkDataFrame,**kwargs)-> SparkDataFrame:      
      for col_info in kwargs['column_names']:
        column_name = col_info['name']
        reference_column = col_info['reference_column']
        mapped_table = col_info['mapped_table']
        common_key = col_info['common_key']
        self.df = self.df.alias('source')
        reference_df = self.spark_session.read.parquet(mapped_table, header=True, sep=';', inferSchema=True).select(common_key, reference_column).alias('reference')
        self.df = self.df.join(reference_df, on=self.df[f'source.{common_key}'] == reference_df[f'reference.{common_key}'], how="left")
        self.df = (self.df.withColumn(f"{column_name}_not_consistent_with"
          , F.col(f'source.{column_name}') != F.col(f'reference.{reference_column}')))
        self.df = self.df.drop(f'reference.{reference_column}')
      return self.df

    def Annotate(self) -> SparkDataFrame:
      expectation_funcs = {
        'not_null': self.Annotate_missing,
        'not_duplicated': self.Annotate_duplicated,
        'timeliness': self.Annotate_outdated,
        'outside_of_rules': self.Annotate_not_rules,
        'in_list': self.Annotate_not_in_list,  
        'consistency': self.Annotate_not_consistent_with,      
     }
      get_cols = self.get_column_tests()
      for test_name, columns in get_cols.items():
        if test_name not in expectation_funcs:
          raise ValueError(f'Unsupported expectation type: {test_name}')
        for col_info in columns:
          column_name = col_info.get('column')
          kwargs = col_info.get('kwargs', {})
          self.df = expectation_funcs[test_name](column_name, **kwargs)          
      return self.df


    

           
          



