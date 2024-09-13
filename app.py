# Commented out IPython magic to ensure Python compatibility.
# %cd /spark-data/capstone_crm/etl

# %run 'load_data.ipynb'
# %run 'verify_data.ipynb'
# %run 'data_cleaning.ipynb'

# %cd /spark-data/capstone_crm/utils
# %run 'logger_setup.ipynb'

# setup for log file
log_file = os.path.join("../logs", "pipeline.log")
setup_logging(log_file)

"""## Data Loading from CSV file to DataFrame"""

# load data into data dictionary
data_files = ["customers", "interactions", "products", "sales_team", "transactions", "country_codes"]
data_dictionary = load_data(data_files)

#check the csv files are loaded successfully or not.
data_dictionary

"""## Join Customers DataFrame to CountryCode DataFrame"""

# join customer df with country code df
data_dictionary["customers"] = join_customer_country_code(data_dictionary["customers"],data_dictionary["country_codes"])

"""## Check the Schema of DataFrames to Insure that Data is successfully loaded into dataFrames"""

# print data
print_schema(data_dictionary)

"""## Verify Data Accuracy"""

# verify data accuracy
if verify_data_accuracy(data_dictionary):
    print("Data accuracy Verified.")
else:
    print("Data accuracy is not Verified.")

"""## Data Cleaning"""

# data cleaning
data_dictionary = clean_data(data_dictionary)

# show cleaned data
show_data(data_dictionary)