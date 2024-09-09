import os
import shutil
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType,BooleanType,IntegerType


# Creating Spark session
def spark_session():
    try:
        spark = SparkSession.builder.appName("load_data").getOrCreate()
        return spark
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}", exc_info=True)
        raise



# Read CSV file
def read_csv_files(file_path):
    try:
        spark = spark_session()
        schema, header = get_appropriate_schema(file_path)
        df = spark.read.format("csv") \
            .option("header",header) \
            .option("enforceSchema", False) \
            .load(file_path, schema=schema)
        return df
    except FileNotFoundError as e:
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
    except Exception as e:
        logging.error(f"Error saving DataFrame to {output_path}: {e}", exc_info=True)
        raise


def get_appropriate_schema(file_path):
    try:
        # Extract file name from path
        file_with_extension = file_path.split('/')[-1]
        file_name = file_with_extension.split('.')[0]
        
        # get the schema for the particular csv file
        schema_dictionary = get_schemas()
        schema = schema_dictionary[file_name]
        #return schema and header
        if file_name=="country_codes":
            return (schema,False)
        else:
            return (schema,True)
    except Exception as e:
        logging.error(f"Error in getting schema for :{file_path}: {e}", exc_info=True)
        raise
        
def get_schemas():
    # Define the schema for each dataFrame
    try:
        schemas = {
            "customers": StructType([
                StructField("Customer_ID", StringType(), True),
                StructField("Name", StringType(), True),
                StructField("Email", StringType(), True),
                StructField("Phone", StringType(), True),
                StructField("Country", StringType(), True)
            ]),
            
            "products": StructType([
                StructField("Product_ID", IntegerType(), True),
                StructField("Product_Name", StringType(), True),
                StructField("Category", StringType(), True),
                StructField("Price", DoubleType(), True)
            ]),
            
            "transactions": StructType([
                StructField("Transaction_ID", StringType(), True),
                StructField("Customer_ID", StringType(), True),
                StructField("Product_ID", IntegerType(), True),
                StructField("Date", DateType(), True),   
                StructField("Amount", DoubleType(), True),
                StructField("Sales_Rep_ID", StringType(), True)
            ]),
            
            "interactions": StructType([
                StructField("Interaction_ID", StringType(), True),
                StructField("Customer_ID", StringType(), True),
                StructField("Interaction_Date", DateType(), True),   
                StructField("Interaction_Type", StringType(), True),
                StructField("Issue_Resolved", BooleanType(), True) 
            ]),
            
            "sales_team": StructType([
                StructField("Sales_Rep_ID", StringType(), True),
                StructField("Name", StringType(), True),
                StructField("Region", StringType(), True),
                StructField("Sales_Target", DoubleType(), True),
                StructField("Sales_Achieved", DoubleType(), True)
            ]),
            # Define the schema for the country codes
            "country_codes": StructType([
                        StructField("Country", StringType(), True),
                        StructField("Country_Code", StringType(), True)
                    ])
        }
        
        return schemas
    except Exception as e:
        logging.error(f"Error in creating schema: {e}", exc_info=True)
        raise
        
        