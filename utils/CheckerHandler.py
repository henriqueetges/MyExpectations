import logging
import time
import inspect
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from .Checker import Checker
from .logging_utils import setup_logging


setup_logging('checker_handler.log')

class CheckerHandler:
    def __init__(self, spark_session, dfs):
      self.spark = spark_session
      self.dfs = dfs  

    def _log_step(self, step: str, message: str) -> None:
      """
      Logs the steps performed

      Parameters:
        step: str
        message: str
      """
      caller = inspect.stack()[1].function
      logging.info({'step': step,  'caller': caller, 'message': message})

    def _log_duration(self, step: str, start_time: float) -> float:
      """
      Logs the duration of the step

      Parameters:
        step: 
        start_time:
      """
      duration = time.time() - start_time
      self._log_step(step, f"completed in {duration:.2f} seconds")
      return duration

    def _extract_metadata(self, table_info: dict) -> tuple:
      """
      Extracts the metadata from the table_info

      Parameters:
        table_info
      """
      return (
          table_info.get('layer'),
          table_info.get('table_name'),
          table_info.get('metadata_path'),
          table_info.get('dataframe')
      )

    def _annotate_dataframe(self, df: SparkDataFrame, metadata_path: str) -> SparkDataFrame :
      """
      Instantiates the Checker class so that it can run 
      the checks against its columns

      Parameters:
        df
        metadata_path:
      """
    
      checker = Checker(self.spark, df, metadata_path)
      return checker.Annotate()

    def _select_standard_columns(self, df: SparkDataFrame, layer: str, table_name: str):
      """
      Selects the result column from the validations dataframe
      """
      return df.select(
          F.lit(layer).alias('layer'),
          F.lit(table_name).alias('table_name'),
          'row_id', 'test_type', 'mandate', 'column',
          'run_date', 'check_result', 'check_score'
      )

    def _process_table(self, table_info: dict) -> SparkDataFrame:
      """
      Processes the table by fetching its metadata, calling the annotation method
      to perform checks and selecting the result columns
      """
      layer, table_name, metadata_path, df = self._extract_metadata(table_info)
      self._log_step("Metadata", f"Fetching metadata for {table_name}")
      self._log_step("Metadata", f"{table_name} metadata fetched")
      self._log_step("Checks", f"{layer}.{table_name} using info from {metadata_path}")
      try:
          start = time.time()
          annotated_df = self._annotate_dataframe(df, metadata_path)
          self._log_duration(f"Checks | {layer}.{table_name}", start)
          return self._select_standard_columns(annotated_df, layer, table_name)
      except Exception as e:
          logging.error(f"Checks | {layer}.{table_name}: Failed to annotate")
          logging.error(str(e))
          return None

    def _compile_results(self, results: list[SparkDataFrame]) -> SparkDataFrame:
      """
      Union all the tables and their tests into a single dataframe
      """
      self._log_step("Compilation", "Compiling results")
      start = time.time()
      final_df = reduce(lambda df1, df2: df1.unionByName(df2), results)
      self._log_duration("Compilation", start)
      return final_df

    def run_checks(self):
      """
      Main function that serves as a handle for processing tables, adding results into 
      the result list and compling results.
      """
      results = []
      for table_info in self.dfs.values():
        result = self._process_table(table_info)
        if result is not None:
          results.append(result)
        else:
          logging.warning('Compilation | No results to compile')
          return None
      return self._compile_results(results)
