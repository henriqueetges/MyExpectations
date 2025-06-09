import yaml

class ConfigLoader:

  def __init__(self, config_path):
    self.config_path = config_path
    self.load_config()

  def load_config(self):
    with open(self.config_path, 'r') as f:
      raw_config = yaml.safe_load(f)
      self.config = raw_config.get('tables')
    
    