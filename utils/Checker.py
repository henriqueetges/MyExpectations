import yaml
import datetime
import pyspark.sql.functions as F
from pyspark.sql.functions import trim, lower
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.types as SType

class Checker:
    def __init__(self, spark_session: SparkSession, df: SparkDataFrame, metadata_path: str):
        self.df = df.withColumn('row_id', F.monotonically_increasing_id())
        self.spark_session = spark_session
        with open(metadata_path, 'r') as f:
          self.table_config = yaml.safe_load(f)        

    
    def get_column_tests(self) -> list[dict]:
      """
      Parses the yaml file and returns a list of key values which are
      easier to use when performing the checks

      Parameters:
        None
      """
      test_cols = [
      {
        'column': col.get('column_name'),
        'type': col.get('type'),
        'mandate': col.get('mandate'),
        'test_type': test.get('test_type', ''),    
        'kwargs': test.get('kwargs', {})
      } 
      for col in self.table_config.get('columns', [])
      for test in col.get('tests', [])]
      return test_cols
    
    def _build_result(self, result_col, score, **kwargs) -> SparkDataFrame:
      """
      BUilds the result dataframe, based on the tests performed against each of the columns

      Parameters:
        result_col: str
        score: str
        kwargs: dict
      """
      column_name = kwargs.get('column', '')
      test_type = kwargs.get('test_type', '')   
      mandate = kwargs.get('mandate', '')
      return self.df.select(
        'row_id', 
        F.lit(test_type).alias('test_type'), 
        F.lit(mandate).alias('mandate'),
        F.lit(column_name).alias('column'),
        F.lit(datetime.date.today()).alias('run_date'),
        result_col.alias('check_result'),
        F.lit(score).alias('check_score')
      )
        
    def Annotate_missing(self, **kwargs) -> SparkDataFrame: 
      """
      Checks a column for missing value, stores results in a dataframe.

      Parameters:
        kwargs: dict
      """
      column_name = kwargs.get('column', '')
      result_flag = ((F.col(column_name).isNull()) | (F.col(column_name).cast('string') == '0.0'))
      score = F.when(result_flag, F.lit(0)).otherwise(F.lit(1))
      return self._build_result(result_flag, score,**kwargs)

    def Annotate_duplicated(self, **kwargs) -> SparkDataFrame:
      """
      Checks a column for duplicated values, stores results in a dataframe

      Parameters:
        kwargs: dict 
      """
      column_name = kwargs.get('column', '')
      window = Window.partitionBy(column_name).orderBy(F.lit(1))
      self.df = self.df.withColumn("row_number", F.row_number().over(window))
      result_flag = (F.col("row_number") > 1)
      score = F.when(result_flag, F.lit(0)).otherwise(F.lit(1))
      return self._build_result(result_flag, score,**kwargs)

    def Annotate_outdated(self, **kwargs) -> SparkDataFrame:
      """
      Checks a column for outdated records, stores results in a dataframe

      Parameters:
        kwargs: dict

      THIS NEEDS TO BE REFACTORED TO CHECK FOR VALUE IN RANGE 
      """
      column_name = kwargs.get('column', '')
      threshold = kwargs.get('kwargs', {}).get('threshold', 0)
      today = datetime.date.today()
      result_flag = (F.col(column_name) < today - F.expr(f"INTERVAL {threshold} DAYS"))
      # change scoring here based on the degree of change to the expected date
      score = F.when(result_flag, F.lit(0)).otherwise(F.lit(1))      
      return self._build_result(result_flag, score,**kwargs)


    def Annotate_not_rules(self, **kwargs) -> SparkDataFrame:
      """
      Checks if a column matched an expression criteria, stores results in a dataframe.

      Parameters:
        kwargs: dict
      """
      column_name = kwargs.get('column')        
      expression = kwargs.get('kwargs', {}).get('expression', {})
      result_flag = (F.when(F.col(column_name).isNull(), F.lit(True)).otherwise(F.expr(expression)))
      score = F.when(result_flag, F.lit(0)).otherwise(F.lit(1))
      return self._build_result(result_flag, score,**kwargs)

    def Annotate_not_in_list(self, **kwargs) -> SparkDataFrame:
      """
      Checks if a column's values are within expected values, stores results in a dataframe.

      parameters:
        kwargs: dict
      """
      column_name = kwargs.get('column')      
      expected_values = kwargs.get('kwargs',{}).get('expected_values', [])
      result_flag = (lower(trim(F.col(column_name))).isin([v.lower() for v in expected_values]))
      score = F.when(result_flag, F.lit(1)).otherwise(F.lit(0))
      return self._build_result(result_flag, score,**kwargs)

    def Annotate_pattern_inconsistency(self, **kwargs):
      """
      Checks if a column doesn't follow a reget pattern, stores results in a dataframe

      Parameters:
        kwargs: dict
      """
      column_name, pattern = kwargs.get('column'), kwargs.get('pattern')
      if column_name is None or pattern is None:
        raise ValueError('Unespecified column or pattern')
      result_flag = (F.when(~F.col(column_name).rlike(pattern), True).otherwise(False))
      score = F.when(result_flag, F.lit(0)).otherwise(F.lit(1))
      return self._build_result(result_flag, score,**kwargs)

    
    def Annotate_type_inconsistency(self, **kwargs):
      """
      Checks if a column doesn't follow the specified type, stores results in a Dataframe

      Parameters:
        kwargs: dict
      """
      column_name, expected_type = kwargs.get('column'), kwargs.get('type')
      if column_name is None or expected_type is None:
        raise ValueError('Unespecified column or column type')
      casted = F.col(column_name).cast(expected_type)      
      result_flag = casted.isNull() & F.col(column_name).isNotNull()  
      score = F.when(result_flag, F.lit(0)).otherwise(F.lit(1))
      return self._build_result(result_flag, score,**kwargs)

      
     

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
      """
      Mapping function that takes the column metadata dictionary and
      calls the appropriate function on the column depending on the 
      tests assigned to it. 

      Aggregates the individual dataframes into a resulting dataframe 
      with all checks
      """
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
      dfs = []
      tests = self.get_column_tests()
      for test_params in tests:
        if test_params.get('test_type') not in expectation_funcs:
          raise ValueError(f'Unsupported expectation type: {test_params.get("test_type")}')
        else:
            df = expectation_funcs[test_params.get('test_type')](**test_params)
            dfs.append(df)
      final_df = dfs[0]
      for df in dfs[1:] :
        final_df = final_df.unionByName(df)
      return final_df
            


    

           
          



