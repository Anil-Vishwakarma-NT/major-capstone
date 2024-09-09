# Commented out IPython magic to ensure Python compatibility.
# %cd /spark-data/etl

# %run 'load_data.ipynb'
# %run 'verify_data.ipynb'
# %run 'data_cleaning.ipynb'
# % run 'customer_purchase_behaviour_analysis.ipynb'

from .etl.load_data import load_data,print_schema
from .etl.verify_data import verify_data_accuracy
from .etl.data_cleaning import clean_data
from .utils.read  import show_data

data_files = ["customers", "interactions", "products", "sales_team", "transactions", "country_codes"]

data_dictionary = load_data(data_files)

data_dictionary

data_dictionary["customers"] = join_customer_country_code(data_dictionary["customers"],data_dictionary["country_codes"])

print_schema(data_dictionary)

show_data(data_dictionary)

if verify_data_accuracy(data_dictionary):
    print("Data accuracy Verified.")
else:
    print("Data accuracy is not Verified.")

data_dictionary = clean_data(data_dictionary)

show_data(data_dictionary)