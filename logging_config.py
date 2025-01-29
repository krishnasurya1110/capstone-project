import logging
import os
import sys

log_directory = "logs"  # Folder name
os.makedirs(log_directory, exist_ok=True)  # Create folder if it doesn't exist

# Create a logger
logger = logging.getLogger("main_logger")
logger.setLevel(logging.DEBUG)  # Set the base logging level

# Create a formatter
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Create a file handler to save logs to app.log
file_handler = logging.FileHandler(os.path.join(log_directory, "app.log"), mode="a")
file_handler.setFormatter(formatter)

# Create a stream handler to print logs to the terminal
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)

# Add both handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(stream_handler)
