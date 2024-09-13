import logging

def setup_logging(log_file):
    logging.getLogger(__name__)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s : %(message)s',
        handlers=[
            logging.FileHandler(log_file),  # Log to file
            logging.StreamHandler()  # Log to console
        ]
    )

import os

log_file = os.path.join("../logs", "pipeline.log")
setup_logging(log_file)
