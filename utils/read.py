import os
import sys
import shutil
import logging
from pyspark.sql import SparkSession
# Add the parent directory of the current script to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from utils.get_config_data import get_schema

# Creating Spark session
def spark_session():
    try:
        
        LOG4J_PATH = "/spark-data/capstone_crm/utils/log4j.properties"
        spark = SparkSession.builder \
            .appName("CRM_PROJECT") \
            .config("spark.driver.extraJavaOptions", f"-Dlog4j.configuration=file:\\{LOG4J_PATH}") \
            .config("spark.executor.extraJavaOptions", f"-Dlog4j.configuration=file:\\{LOG4J_PATH}") \
            .getOrCreate()
        return spark
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}", exc_info=True)
        raise

# spark session
spark = spark_session()

# get appropriate schema
def get_appropriate_schema_and_header(file_path):
    try:
        # Extract file name from path
        file_with_extension = file_path.split('/')[-1]
        file_name = file_with_extension.split('.')[0]
        
        # get schema from get_config_data file
        schema = get_schema(file_name)
        
        if file_name=="country_codes":
            return (schema,False)
        else:
            return (schema,True)
    except Exception as e:
        logging.error(f"Error in getting schema for :{file_path}: {e}", exc_info=True)
        raise
        


# Read CSV file
def read_csv_files(file_path):
    try:
        schema, header = get_appropriate_schema_and_header(file_path)
        df = spark.read.format("csv") \
            .option("header",header) \
            .load(file_path, schema=schema)
        return df
    except FileNotFoundError as e:
        logging.error(f"Error reading CSV file at {file_path}: {e}", exc_info=True)
        raise

    except Exception as e:
        logging.error(f"Error reading CSV file at {file_path}: {e}", exc_info=True)
        raise
        



# Save cleaned dataFrame as a single CSV file
def save_as_single_csv(df, output_path):
    try:
        temp_dir = output_path + "_temp" 
        # Write DataFrame as CSV with a single partition
        df.coalesce(1).write.mode('overwrite').csv(temp_dir, header=True)
        # Move the CSV file to the desired output path
        temp_csv_file = [file for file in os.listdir(temp_dir) if file.endswith('.csv')][0]
        shutil.move(os.path.join(temp_dir, temp_csv_file), output_path)
        # Clean up temporary directory
        shutil.rmtree(temp_dir)
        # logging.info(f"DataFrame saved successfully as {output_path}")
    except FileNotFoundError as e:
        logging.error(f"Error reading CSV file at {file_path}: {e}", exc_info=True)
        raise
    except Exception as e:
        logging.error(f"Error saving DataFrame to {output_path}: {e}", exc_info=True)
        raise



