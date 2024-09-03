from pyspark.sql.types import StructType, StructField, StringType
import sys
import os
import logging

sys.path.append(os.path.abspath('..'))
from utils.read import read_csv_files

# Define the path variable
RAW_DATA_PATH = "/spark-data/data/raw_data/"

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Extract data from raw data folder
def load_data(data_files):
    data_dictionary = {}
    for data_file in data_files:
        try:
            logging.info(f"Loading data for {data_file}")

            # Read raw data
            if data_file == "country_codes":
                # Define the schema for the country codes
                country_schema = StructType([
                    StructField("Country", StringType(), True),
                    StructField("Country_Code", StringType(), True)
                ])
                # Read country code df
                data_dictionary[f"{data_file}"] = read_csv_files(RAW_DATA_PATH,f"{data_file}.csv", country_schema)
                logging.info(f"{data_file} data loaded successfully")
            else:
                data_dictionary[f"{data_file}"] = read_csv_files(RAW_DATA_PATH,f"/spark-data/data/raw_data/{data_file}.csv")
                logging.info(f"{data_file} data loaded successfully.")
        except Exception as e:
            logging.error(f"Error loading data for {data_file}: {e}", exc_info=True)
    
    return data_dictionary

# Used to print schemas
def print_schema(data_dictionary):
    try:
        logging.info("Printing schemas for all dataframes")
        for dataframe in data_dictionary.values():
            dataframe.printSchema()
    except Exception as e:
        logging.error("Error printing schemas", exc_info=True)


# Show data 
def show_data(data_dictionary):
    try:
        for name, dataframe in data_dictionary.items():
            logging.info(f"Showing data for {name}")
            dataframe.show(5)
    except Exception as e:
        logging.error(f"Error showing data: {e}", exc_info=True)
        raise