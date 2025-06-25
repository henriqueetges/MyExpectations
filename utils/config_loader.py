import yaml
import pandas as pd
class ConfigLoader:

  def __init__(self, config_path, config_type):
    self.config_path = config_path
    self.config_type = config_type
    self.load_config()

  def load_config(self):
    """
    Loads configurations from the yaml file.
    """
    with open(self.config_path, 'r') as f:
      raw_config = yaml.safe_load(f)
      if self.config_type == 'tables':
        self.config = raw_config.get('tables')
      else:
        self.config = raw_config.get('columns')
  
  def print_configs(self):
    """
    Makes printing the configs prettier in a notebook
    """
    df = pd.DataFrame(self.config)
    if self.config_type != 'tables':
      df = df.explode('tests')
    return df
    