
import logging
from pathlib import Path
import os

def setup_logging(log_file: str = "checker_handler.log", level=logging.INFO):
    """
    Configures logging to output to both a file and the notebook/console.
    Saves the log file in a 'logs' folder one level up from the current script.

    Parameters:
      log_file (str): Name of the log file.
      level (int): Logging level (e.g., logging.INFO, logging.DEBUG).
 """
    try:
      current_file = Path(__file__).resolve()
      parent_dir = current_file.parent.parent
    except NameError:
      parent_dir = Path(os.getcwd()).resolve().parent  # Fallback if __file__ is not defined

    logs_dir = os.path.join(parent_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    log_path = os.path.join(logs_dir, log_file)  
    for handler in logging.root.handlers[:]:
      logging.root.removeHandler(handler)

    
    logging.basicConfig(
      level=level,
      format='%(asctime)s | %(levelname)s | %(message)s',
      handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler()])
            

