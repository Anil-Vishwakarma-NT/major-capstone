import os
import shutil
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# Creating Spark session
def spark_session():
    try:
        spark = SparkSession.builder.appName("load_data").getOrCreate()
        return spark
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}", exc_info=True)
        raise



# Read CSV file
def read_csv_files(file_path, schema=None, header=False, inferSchema=False):
    try:
        
        if schema is None:
            inferSchema = True
            header = True

        spark = spark_session()
        df = spark.read.format("csv") \
            .option("header", header) \
            .option("inferSchema", inferSchema) \
            .option("enforceSchema", False) \
            .load(file_path, schema=schema)
        return df
    except Exception as e:
        logging.error(f"Error reading CSV file at {file_path}: {e}", exc_info=True)
        raise



# Save cleaned dataframe as a single CSV file
def save_as_single_csv(df, output_path):
    try:
        temp_dir = output_path + "_temp"
        # logging.info(f"Saving dataframe to {output_path}")
        
        # Write DataFrame as CSV with a single partition
        df.coalesce(1).write.mode('overwrite').csv(temp_dir, header=True)
        
        # Move the CSV file to the desired output path
        temp_csv_file = [file for file in os.listdir(temp_dir) if file.endswith('.csv')][0]
        shutil.move(os.path.join(temp_dir, temp_csv_file), output_path)
        
        # Clean up temporary directory
        shutil.rmtree(temp_dir)
        
        # logging.info(f"DataFrame saved successfully as {output_path}")
    except Exception as e:
        logging.error(f"Error saving DataFrame to {output_path}: {e}", exc_info=True)
        raise

