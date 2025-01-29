import logging
import os

log_directory = "logs"  # Folder name
os.makedirs(log_directory, exist_ok=True)  # Create folder if it doesn't exist

logging.basicConfig(
    filename=os.path.join(log_directory, "app.log"),  # Save inside 'logs/' folder
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filemode="a" # "a" means append mode
)

logger = logging.getLogger("main_logger")
