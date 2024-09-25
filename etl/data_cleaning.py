import sys
import os
sys.path.append(os.path.abspath('..'))
import logging
from utils.read_write import save_as_single_csv
from utils.get_config_data import CLEANED_DATA_DIR
from pyspark.sql.functions import col,when,count,mean,regexp_replace,split, to_date, initcap, lit, trim
from datetime import datetime
def clean_data(data_dictionary):
    for data_file_name in data_dictionary:
        try:
            logging.info(f"Cleaning {data_file_name} dataframe")
            
            # Identify missing values from the dataframe
            logging.info("Checking for missing values")
            check_for_missing_values(data_dictionary[data_file_name])

            # Handle missing values
            logging.info("Handling missing values")
            data_dictionary[data_file_name] = handle_missing_values(data_dictionary[data_file_name])

            # Remove duplicates
            logging.info("Removing duplicates")
            data_dictionary[data_file_name] = remove_duplicated(data_dictionary[data_file_name])

            # Correct inaccurate data
            logging.info("Correcting inaccurate data")
            data_dictionary[data_file_name] = accurate_data(data_file_name, data_dictionary[data_file_name])
            logging.info("Successfully cleaned data for customers dataFrame")
        except Exception as e:
            logging.error(f"Error cleaning {data_file_name} dataFrame: {e}", exc_info=True)
            sys.exit("Exiting due to an error in cleaning data")
            
        logging.info(".....................................................................................................................")            

    # Save cleaned dataframes into cleaned data folder as a csv file
    try:
        logging.info("Saving cleaned dataframes")
        save_as_csv(data_dictionary)
    except FileNotFoundError as e:
        logging.error(f"File not found for: {e}", exc_info=True)
        sys.exit("Exiting due to file not found")    
    except Exception as e:
        logging.error(f"Error saving cleaned dataframes: {e}", exc_info=True)
        sys.exit("Exiting due to an unexpected error")
    
    return data_dictionary
# Check missing values in dataFrame
def check_for_missing_values(dataframe):
    try:
        #checking missing values in dataframe
        dataframe.select([count(when(col(column).isNull(), column)).alias(column) for column in dataframe.columns])
    except Exception as e:
        logging.error("Error checking for missing values", exc_info=True)
        raise
# Handle missing values
def handle_missing_values(dataframe):
    try:
        # Loop through columns and apply transformations
        for column_name, column_type in dataframe.dtypes:
            # handle the missing values with "unknown"
            if column_name in ["Name", "Email", "Phone", "Country_Code", "Region", "Issue_Resolved", "Interaction_Type", "Country", "Category"]:
                dataframe = dataframe.na.fill({column_name: 'Unknown'}) 
            
            elif column_name in ["Price","Amount","Sales_Achieved","Sales_Target"]:
                # Find the avg of column_name
                avg_price = dataframe.select(mean(col(column_name))).first()[0]
                # Fill avg of column_name at null place
                dataframe = dataframe.na.fill({column_name: avg_price})  
        
        return dataframe            
                    
    except Exception as e:
        logging.error("Error handling missing values", exc_info=True)
        raise
# Removing duplicate data
def remove_duplicated(dataframe):
    try:
        logging.info(f"{dataframe.count()} total records.")
        dataframe = dataframe.dropDuplicates()
        logging.info(f"{dataframe.count()} records left after dropping duplicates.")
        return dataframe
        
    except Exception as e:
        logging.error("Error removing duplicates", exc_info=True)
        raise
    
    
# Filtering and formatting dataFrame
def accurate_data(data_file_name, dataframe):
    try:
        if data_file_name == "customers":
            dataframe = correct_customer_data(dataframe)
        else:
            dataframe = correct_inaccurate_data(dataframe)
            
        return dataframe    
    except Exception as e:
        logging.error(f"Error correcting inaccurate data for {data_file_name}", exc_info=True)
        raise

def correct_customer_data(dataframe):
    try:
        # Define a regex pattern for valid emails
        email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z]+\.(com|net|org|edu|gov|mil|co|in|info)$'
        #email_regex = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+\.[A-Za-z]{2,}$' define the top level domain as at least 2 characters
        
        # Loop through columns and apply transformations
        for column_name, column_type in dataframe.dtypes:
            # If column is of string type, apply trim
            if column_type == 'String' and column_name not in ["Customer_ID"]:
                dataframe = dataframe.withColumn(column_name, trim(col(column_name)))
                
            if column_name in ["Name", "Country"]:
                dataframe = dataframe.withColumn(column_name, initcap(column_name))
                
            elif column_name == "Phone":
                # Process Phone column and remove extensions 
                dataframe = dataframe.withColumn(column_name, split(col(column_name), "x")[0])
                # remove +1 from the prefix
                dataframe = dataframe.withColumn(column_name,
                    when(col(column_name).startswith("+1"),col(column_name).substr(2,50)) 
                    .otherwise(col(column_name))
                )
                # remove the invalid country code (001)
                dataframe = dataframe.withColumn(column_name,when(col(column_name).startswith("001"),
                            col(column_name).substr(3, 100)).otherwise(col(column_name))
                )
                # remove non numeric things, function regexp_replace(column_name, condition, action)
                dataframe = dataframe.withColumn(
                    column_name,
                    when(col(column_name) != "Unknown", regexp_replace(col(column_name), r"[^0-9]", ""))   # [^0-9] non numeric (^ non if inside bracket)
                    .otherwise(col(column_name))
                )
                
            # email formatting    
            elif column_name == "Email":
                # Replace invalid emails with "NA"
                dataframe = dataframe.withColumn(column_name,
                    when(col(column_name).rlike(email_regex), col(column_name)).otherwise("Unknown")
                )
                
        return dataframe        

    except Exception as e:
        logging.error("Error formatting customer data", exc_info=True)
        raise
    

def correct_inaccurate_data(dataframe):
    try:
        # Get the current date in the proper format for date comparisons
        current_date = datetime.now().date()
        current_date_str = current_date.strftime('%Y-%m-%d')

        # Loop through columns and apply operations based on column name and data type
        for column_name, column_type in dataframe.dtypes:
            
            # If column is of string type, apply trim
            if column_type == 'String' and column_name not in ["Interaction_ID","Product_ID","Sales_Rep_ID","Transaction_ID"]:
                dataframe = dataframe.withColumn(column_name, initcap(trim(col(column_name))))

            # Check for specific columns to apply additional transformations
            
            # If the column is "Amount", filter out negative values
            if column_name in ["Amount","Price","Sales_Target","Sales_Achieved"] :
                dataframe = dataframe.filter(col(column_name) > 0)
            
            # Standardize the "Date" and "Interaction_Date" columns
            elif column_name in ["Date", "Interaction_Date"]:
                # formate in "YYYY-MM-DD" formate
                dataframe = dataframe.withColumn(column_name, to_date(col(column_name), "yyyy-MM-dd"))
                # Check that if date is greater then current date then replace it with "current date"
                dataframe = dataframe.withColumn(column_name,when(col(column_name) < lit(current_date_str),
                                                                col(column_name)).otherwise(lit(current_date_str))) 
        return dataframe        
    except Exception as e:
        logging.error("Error correcting inaccurate data", exc_info=True)
        sys.exit("Exiting due error in data cleaning")
    
    
# Join customer df with country code df and get the joined dataFrame
def join_customer_country_code(customer_df, country_codes_df):
    try:
        # Join cleaned customer df and country_codes_df
        join_customer_df = customer_df.join(country_codes_df, 'Country', "left")
        customer_df_columns = customer_df.columns
        customer_df = join_customer_df.select(*customer_df_columns, 'Country_Code')
        return customer_df
    except Exception as e:
        logging.error("Error joining customer and country code data", exc_info=True)
    
# Save cleaned dataFrame into csv file
def save_as_csv(data_dictionary):
    
    for data_file_name in data_dictionary:
        try:
            save_as_single_csv(data_dictionary[data_file_name], f"{CLEANED_DATA_DIR}/{data_file_name}.csv")
            logging.info(f"{data_file_name} saved successfully")
        except Exception as e:
            logging.error(f"Error saving {data_file_name} to CSV", exc_info=True)
            raise