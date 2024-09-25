import yaml
import logging
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType,BooleanType,IntegerType

# config file path
config_path = "/spark-data/capstone_crm/config/base_config.yaml"

# Load YAML configuration
def load_yaml_config(config_path):
    try:
        with open(config_path, 'r') as file:   # with is used to close the file after reading file
            config = yaml.safe_load(file)   # return python dictionary
        return config
    except Exception as e:
        logging.error(f"Error loading YAML configuration: {e}", exc_info=True)
        raise

# load yaml data into python dictionary
yaml_config = load_yaml_config(config_path)
# get file directories
RAW_DATA_PATH = yaml_config["RAW_DATA_PATH"]
CLEANED_DATA_DIR = yaml_config["CLEANED_DATA_DIR"]


# Map string types from YAML to PySpark DataTypes
def get_data_type(data_type_str):
    data_type_mapping = {
        "String": StringType(),
        "Integer": IntegerType(),
        "Double": DoubleType(),
        "Date": DateType(),
        "Boolean": BooleanType()
    }
    # Default to StringType if no match
    return data_type_mapping.get(data_type_str, StringType()) 


# Convert YAML schema to PySpark StructType
def convert_yaml_schema_to_struct(yaml_schema):
    fields = [StructField(field['name'], get_data_type(field['type']), True) for field in yaml_schema]
    return StructType(fields)

def get_schema(file_name):
    schema_dict = yaml_config['file_schemas']
    # Convert the schema to PySpark StructType
    if file_name not in schema_dict:
        raise ValueError(f"Schema for {file_name} not found in YAML configuration.")
    
    schema = convert_yaml_schema_to_struct(schema_dict[file_name])
    return schema























# import PyYAML library to make python to parse with yaml
# StructType: Defines the schema of a DataFrame as a list of StructField objects.
# StructField: Represents a single field in a schema, including its name, data type, and nullability.