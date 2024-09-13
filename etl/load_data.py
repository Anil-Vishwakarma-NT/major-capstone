import sys
import os
sys.path.append(os.path.abspath('..'))

from utils.read import read_csv_files
import logging

# Define the path variable
RAW_DATA_PATH = "/spark-data/capstone_crm/data/raw_data/"

# Extract data from raw data folder
def load_data(data_files):
    data_dictionary = {}
    for data_file in data_files:
        try:
            # Read raw data
            data_dictionary[f"{data_file}"] = read_csv_files(f"{RAW_DATA_PATH}{data_file}.csv")
            logging.info(f"{data_file} data loaded successfully.")
        except Exception as e:
            logging.error(f"Error loading data for {data_file}: {e}", exc_info=True)
            sys.exit("Exiting due to an unexpected error")

    return data_dictionary

# Used to print schemas
def print_schema(data_dictionary):
    try:
        logging.info("Printing schemas for all dataframes")
        for dataframe in data_dictionary.values():
            dataframe.printSchema()
    except Exception as e:
        logging.error("Error printing schemas", exc_info=True)
        raise

# Show data
def show_data(data_dictionary):
    try:
        for name, dataframe in data_dictionary.items():
            dataframe.show(5)
    except Exception as e:
        logging.error(f"Error showing data: {e}", exc_info=True)
        raise