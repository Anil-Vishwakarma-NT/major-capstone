

import os
import sys
sys.path.append(os.path.abspath('..'))
import matplotlib.pyplot as plt
import logging

from pyspark.sql.functions import sum,col,desc,count,round
from utils.read import read_csv_files
from utils.visualization import bar_graph

# Commented out IPython magic to ensure Python compatibility.
# logger setup
# %cd /spark-data/capstone_crm/utils
# %run 'logger_setup.ipynb'

# Define the path variable
CLEANED_DATA_PATH = "/spark-data/capstone_crm/data/cleaned/"
logging.info("Customer Purchase Analysis................................. ")

try:
    logging.info("Loading customer.csv and transaction.csv")
    # Read customers data
    customers_df = read_csv_files(f"{CLEANED_DATA_PATH}customers.csv")
    # Read transactions data
    transactions_df = read_csv_files(f"{CLEANED_DATA_PATH}transactions.csv")
except FileNotFoundError as e:
    logging.error(f"Error reading CSV files: {e}", exc_info=True)
    # Optionally stop the execution explicitly if required
    sys.exit("Exiting due to file not found error")
except Exception as e:
    logging.error(f"Error reading CSV files: {e}", exc_info=True)
    # Optionally stop the execution explicitly if required
    sys.exit("Exiting due to an unexpected error")

"""# Analyze customer purchase behavior"""

try:
    # get the total spending of the customer
    total_spending_df = transactions_df.groupBy("customer_id").agg(sum("Amount").alias("Total_Spending"),count("*").alias("Purchase_Frequency"))
    #get the average spending of the customer
    total_spending_df = total_spending_df.withColumn("Avg_spending", round(col("Total_Spending") / col("Purchase_Frequency"), 2))
    #join transaction df to customer df
    joined_df = total_spending_df.join(customers_df,'customer_id','inner')
    joined_df = joined_df.select(col("customer_id"),col("Name"),col("total_Spending"),col("Purchase_Frequency"),col("Avg_spending"),col("Country"))
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### Top Customers on total spending"""

try:
    logging.info("top 10 customers on total spending")
    #Get the top 10 customers based on total spending.
    top_ten_customers_df = joined_df.select("Name","total_Spending","Country").orderBy(desc("total_Spending")).limit(10)
    # show data
    top_ten_customers_df.show()
    # Convert the columns from Spark DataFrame to Python lists
    categories = [f"{row['Name']} ({row['Country']})" for row in top_ten_customers_df.collect()]
    values = [row['total_Spending'] for row in top_ten_customers_df.collect()]
    # Create a figure and axes object
    fig, ax = plt.subplots(figsize=(10, 6))
    # Call the bar_graph function
    bar_graph( ax=ax, categories=categories, values=values, title="Top 10 Customers by Total Spending", xlabel="Customers",
        ylabel="Total Spending ($)", color='#3498db', edgecolor='red')
    # Display bar graph
    plt.show()

except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### Top customers who purchase most items"""

try:
    logging.info("Top 10 Customers on Purchase Frequency")
    #calculate purchase frequency per customer
    top_10_frequency_df = joined_df.select("Name","Purchase_Frequency","Country").orderBy(col("Purchase_Frequency").desc()).limit(10)
    top_10_frequency_df.show()
    # Convert the columns from Spark DataFrame to Python lists
    categories = [f"{row['Name']}  ({row['Country']})" for row in top_10_frequency_df.collect()]
    values = [row['Purchase_Frequency'] for row in top_10_frequency_df.collect()]
    # Create figure and axis for the bar graph
    fig, ax = plt.subplots(figsize=(10, 6))  # Customize the size if needed
    # Call the bar_graph function
    bar_graph(ax,categories=categories,values=values,title="Top 10 Customers by Purchase Frequency",xlabel="Customer Name",
        ylabel="Purchase Frequency",color='#3498db',  edgecolor='red')
    # Show bar graph
    plt.show()

except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### Top customer on average purchase"""

try:
    logging.info("Top Customers on Average Purchase")
    #calculate average purchase per customer
    # Order by Purchase Frequency and Limit to Top 10
    top_10_avg_spending = joined_df.select("Name","Avg_spending","Country").orderBy(desc("Avg_spending")).limit(10)
    top_10_avg_spending.show()
    # Convert the columns from Spark DataFrame to Python lists
    categories = [f"{row['Name']} ({row['Country']})" for row in top_10_avg_spending.collect()]
    values = [row['Avg_spending'] for row in top_10_avg_spending.collect()]
    # Create a figure and axes object
    fig, ax = plt.subplots(figsize=(10, 6))
    # Call the bar_graph function
    bar_graph(ax=ax, categories=categories, values=values, title="Top 10 Customers on Average Purchase", xlabel="Customers",
        ylabel="Average Spending", color='#3498db', edgecolor='red')
    # Display bar graph
    plt.show()

except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### Top Countries with maximum customers"""

try:
    logging.info("Top 10 countries with maximum Customer")
    #customer frequency according to country
    # Order by Purchase Frequency and Limit to Top 10
    customer_country_df = joined_df.groupBy("Country").agg(count("customer_id").alias("Customer_volume_Country_wise"), sum("Total_Spending").alias("Total_Country_Spending"))
    top_10_countries_with_maximum_customer_df = customer_country_df.select("Country","Customer_volume_Country_wise").orderBy(col("Customer_volume_Country_wise").desc()).limit(10)
    top_10_countries_with_maximum_customer_df.show()
    # Convert the columns from Spark DataFrame to Python lists
    categories = [row['Country'] for row in top_10_countries_with_maximum_customer_df.collect()]
    values = [row['Customer_volume_Country_wise'] for row in top_10_countries_with_maximum_customer_df.collect()]
    # Create a figure and axes object
    fig, ax = plt.subplots(figsize=(10, 6))
    # Call the bar_graph function
    bar_graph(ax=ax, categories=categories, values=values, title="Top 10 Countries By Customers", xlabel="Country Name",
        ylabel="Customer Volume", color='#3498db', edgecolor='red')
    # Display the plot
    plt.show()

except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

try:
    logging.info("Top 10 countries with maximum Spending")
    #customer frequency according to country
    top_10_countries_with_maximum_spending_df = customer_country_df.select("Country","Total_Country_Spending").orderBy(col("Total_Country_Spending").desc()).limit(10)
    top_10_countries_with_maximum_spending_df.show()
    # Convert the columns from Spark DataFrame to Python lists
    categories = [row['Country'] for row in top_10_countries_with_maximum_spending_df.collect()]
    values = [row['Total_Country_Spending'] for row in top_10_countries_with_maximum_spending_df.collect()]
    # Create a figure and axes object
    fig, ax = plt.subplots(figsize=(10, 6))
    # Call the bar_graph function
    bar_graph(ax=ax, categories=categories, values=values, title="Top 10 Countries By Total spending ", xlabel="Country Name",
        ylabel="Customer Volume", color='#3498db', edgecolor='red')
    # Display bar graph
    plt.show()

except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)