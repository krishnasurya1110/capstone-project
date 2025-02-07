from google.cloud import logging as gcp_logging

# Initialize GCP Logging client and logger
gcp_client = gcp_logging.Client()
logger = gcp_client.logger("ingestion_logger")
