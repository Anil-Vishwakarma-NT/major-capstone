# etl.data_loading.ipynb

import logging
import sys
import os
sys.path.append(os.path.abspath('..'))

from utils.read import save_as_single_csv

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
        
        except Exception as e:
            logging.error(f"Error cleaning {data_file_name} dataframe: {e}", exc_info=True)
        
        logging.info(".....................................................................................................................")    

    # Save cleaned dataframes into cleaned data folder as a csv file
    try:
        logging.info("Saving cleaned dataframes")
        save_as_csv(data_dictionary)
    except Exception as e:
        logging.error(f"Error saving cleaned dataframes: {e}", exc_info=True)
    
    return data_dictionary

# Check missing values in dataFrame
def check_for_missing_values(dataframe):
    try:
        dataframe.select([count(when(col(c).isNull(), c)) \
            .alias(c) for c in dataframe.columns]).show()
    except Exception as e:
        logging.error("Error checking for missing values", exc_info=True)

# Handle missing values
def handle_missing_values(dataframe):
    try:
        if "Email" in dataframe.columns and "Phone" in dataframe.columns:
            # Fill unknown at the place of null value
            dataframe = dataframe.na.fill({'Email': 'NA', 'Phone': "NA"})

        if "Category" in dataframe.columns:
            # Fill unknown at the place of null value in category column
            dataframe = dataframe.na.fill({"Category": "unknown"})

        if "Amount" in dataframe.columns:
            # Find the avg of Amount column
            avg_amount = dataframe.select(mean(col("Amount"))).first()[0]
            # Fill avg of amount column at null place
            dataframe = dataframe.na.fill({"Amount": avg_amount})

        if "Interaction_Type" in dataframe.columns:
            # Fill NA in the interaction type
            dataframe = dataframe.na.fill({'Interaction_Type': "NA"})

        if "Sales_Achieved" in dataframe.columns:
            # Find the avg of Sales_Achieved column
            avg_sales_achieved = dataframe.select(mean(col("Sales_Achieved"))).first()[0]
            # Fill Null values in Sales_Achieved column with avg of the column
            dataframe = dataframe.na.fill({"Sales_Achieved": avg_sales_achieved})

        if "Country_Code" in dataframe.columns:
            dataframe = dataframe.na.fill({'Country_Code': 'NA'})

        # Check if there are any missing values
        check_for_missing_values(dataframe)
    except Exception as e:
        logging.error("Error handling missing values", exc_info=True)
    
    return dataframe

# Removing duplicate data
def remove_duplicated(dataframe):
    try:
        logging.info(f"{dataframe.count()} total records.")
        dataframe = dataframe.dropDuplicates()
        logging.info(f"{dataframe.count()} records left after dropping duplicates.")
    except Exception as e:
        logging.error("Error removing duplicates", exc_info=True)
    
    return dataframe

# Filtering and formatting dataFrame
def accurate_data(data_file_name, dataframe):
    try:
        if data_file_name == "customers":
            dataframe = correct_customer_data(dataframe)
        else:
            dataframe = correct_inaccurate_data(dataframe)
    except Exception as e:
        logging.error(f"Error correcting inaccurate data for {data_file_name}", exc_info=True)
    
    return dataframe

# Formatting customer data
def correct_customer_data(dataframe):
    try:
        dataframe = dataframe.withColumn("Phone", split(col("Phone"), "x")[0])
        # Remove the '+1' prefix if it exists
        dataframe = dataframe.withColumn(
            "Phone",
            when(col("Phone").startswith("+1"),
                col("Phone").substr(2, 100)
            ).otherwise(col("Phone"))
        )
        dataframe = dataframe.withColumn(
            "Phone",
            when(col("Phone").startswith("001"),
                col("Phone").substr(3, 100)
            ).otherwise(col("Phone"))
        )
        # Remove all non-numeric values
        dataframe = dataframe.withColumn("Phone", 
            when(col("Phone") != "NA", regexp_replace(col("Phone"), r"[^0-9]", "")) \
            .otherwise(col("Phone"))
        )
    except Exception as e:
        logging.error("Error formatting customer data", exc_info=True)
    
    return dataframe

# Filter and format data from dataFrame based on column name
def correct_inaccurate_data(dataframe):
    try:
        if "Amount" in dataframe.columns:
            dataframe = dataframe.filter(col('Amount') > 0)
            # Standardize date formats
        if "Date" in dataframe.columns:
            dataframe = dataframe.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))

        if "Interaction_Date" in dataframe.columns:
            dataframe = dataframe.withColumn("Interaction_Date", to_date(col("Interaction_Date"), "yyyy-MM-dd"))

        if "Name" in dataframe.columns:
            # Capitalize names
            dataframe = dataframe.withColumn("Name", initcap(col("Name")))
    except Exception as e:
        logging.error("Error correcting inaccurate data", exc_info=True)
    
    return dataframe

# Join customer df with country code df and get the joined dataFrame
def join_customer_country_code(customer_df, country_codes_df):
    try:
        # Join cleaned customer df and country_codes_df
        join_customer_df = customer_df.join(country_codes_df, 'Country', "left")
        customer_df_columns = customer_df.columns
        customer_df = join_customer_df.select(*customer_df_columns, 'Country_Code')
    except Exception as e:
        logging.error("Error joining customer and country code data", exc_info=True)
    
    return customer_df

# Save cleaned dataFrame into csv file
def save_as_csv(data_dictionary):
    output_dir = "/spark-data/data/cleaned"
    for data_file_name in data_dictionary:
        try:
            save_as_single_csv(data_dictionary[data_file_name], f"{output_dir}/{data_file_name}.csv")
            logging.info(f"{data_file_name} saved successfully")
        except Exception as e:
            logging.error(f"Error saving {data_file_name} to CSV", exc_info=True)
