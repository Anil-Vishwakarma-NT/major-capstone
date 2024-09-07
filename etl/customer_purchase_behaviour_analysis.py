import os
import sys
sys.path.append(os.path.abspath('..'))
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

from pyspark.sql.functions import sum,col,desc,count,round
from utils.read import read_csv_files
import logging

# Define the path variable
CLEANED_DATA_PATH = "/spark-data/capstone_crm/data/cleaned/"
logging.info("Customer Purchase Analysis................................. ")

try:
    logging.info("loading customer.csv and transaction.csv")
    # read customers data
    customers_df = read_csv_files(f"{CLEANED_DATA_PATH}customers.csv")
    # read transactions data
    transactions_df = read_csv_files(f"{CLEANED_DATA_PATH}transactions.csv")
except FileNotFoundError as e:
        logging.error(f"Error reading CSV files : {e}", exc_info=True)
        raise

"""# Analyze customer purchase behavior"""

try:
    # get the total spending of the customer
    total_spending_df = transactions_df.groupBy("customer_id").agg(sum("Amount").alias("Total_Spending"),count("*").alias("Purchase_Frequency"))
    total_spending_df = total_spending_df.withColumn("Avg_spending", round(col("Total_Spending") / col("Purchase_Frequency"), 2))
    joined_df = total_spending_df.join(customers_df,'customer_id','inner')
    joined_df = joined_df.select(col("customer_id"),col("Name"),col("total_Spending"),col("Purchase_Frequency"),col("Avg_spending"),col("Country"))
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### Top Customers on total spending"""

try:
    logging.info("top 10 customers on total spending")
    #Get the top 10 customers based on total spending.
    top_ten_customers_df = joined_df.select("Name","total_Spending","Country").orderBy(desc("total_Spending")).limit(10)
    top_ten_customers_df.show()
    top_10_customers_pd = top_ten_customers_df.limit(10).toPandas()

    logging.info("plotting top 10 customer on total spending")
    # Plotting the results using Matplotlib - Vertical Bar Char
    # Setting the style
    plt.style.use('seaborn-v0_8-darkgrid')  # You can also try 'ggplot' or 'fivethirtyeight'

    # Create a larger figure
    plt.figure(figsize=(15, 8))

    # Create the bar chart with a gradient color for better aesthetics
    bars = plt.bar(
        top_10_customers_pd['Name'],
        top_10_customers_pd['total_Spending'],
        color=plt.cm.PuBu(np.linspace(0.8, 0.3, len(top_10_customers_pd))),
        edgecolor='darkblue',
        linewidth=1.5,
        alpha=0.9
    )

    # Adding Country Names on Top of Each Bar
    for bar, country in zip(bars, top_10_customers_pd['Country']):
        yval = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width()/2, yval + 0.5,  # Adjust position
            country, ha='center', va='bottom',
            fontsize=10, fontweight='bold', color='darkblue'
        )

    # Labels and Title with custom font and padding
    plt.xlabel('Customer Name', fontsize=14, fontweight='bold', labelpad=15)
    plt.ylabel('Total Spending', fontsize=14, fontweight='bold', labelpad=15)
    plt.title('Top 10 Customers by Total Spending', fontsize=18, fontweight='bold', pad=20)

    # Rotate the x-axis labels for readability
    plt.xticks(rotation=45, ha='right', fontsize=12, color='darkblue')

    # Adding grid lines to enhance readability
    plt.grid(axis='y', linestyle='--', linewidth=0.7, alpha=0.7)

    # Adding a subtle shadow to bars for a 3D effect

    # Customize the spines (axis lines)
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    plt.gca().spines['left'].set_color('darkblue')
    plt.gca().spines['bottom'].set_color('darkblue')

    # Show the plot
    plt.tight_layout()  # Adjusts plot to ensure everything fits nicely
    plt.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### Top customers who purchase most items"""

try:
    logging.info("Top 10 customers on purchase volume")
    #calculate purchase frequency per customer
    # Order by Purchase Frequency and Limit to Top 10
    top_10_frequency_df = joined_df.select("Name","Purchase_Frequency","Country").orderBy(desc("Purchase_Frequency")).limit(10)
    top_10_frequency_df.show()

    # Convert to Pandas DataFrame for Visualization
    top_10_frequency_pd = top_10_frequency_df.toPandas()

    # Plotting the results
    plt.figure(figsize=(12, 8))
    bars = plt.bar(top_10_frequency_pd['Name'], top_10_frequency_pd['Purchase_Frequency'], color='skyblue')

    # Adding Country Names on Top of Each Bar
    for bar, country in zip(bars, top_10_frequency_pd['Country']):
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval, country, ha='center', va='bottom', fontsize=8, fontweight='bold')

    plt.xlabel('Customer Name')
    plt.ylabel('Purchase Frequency')
    plt.title('Top 10 Customers by Purchase Frequency')
    plt.xticks(rotation=45, ha='right')
    plt.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### Top customer on average purchase"""

try:
    logging.info("Top customers on average purchase")
    #calculate average purchase per customer
    # Order by Purchase Frequency and Limit to Top 10
    top_10_avg_spending = joined_df.select("Name","Avg_spending","Country").orderBy(desc("Avg_spending")).limit(10)
    top_10_avg_spending.show()

    # Convert to Pandas DataFrame for Visualization
    top_10_avg_spending_pd = top_10_avg_spending.toPandas()

    # Plotting the results
    plt.figure(figsize=(12, 8))
    bars = plt.bar(top_10_avg_spending_pd['Name'], top_10_avg_spending_pd['Avg_spending'], color='skyblue')

    # Adding Country Names on Top of Each Bar
    for bar, country in zip(bars, top_10_frequency_pd['Country']):
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval, country, ha='center', va='bottom', fontsize=8, fontweight='bold')

    plt.xlabel('Customer Name')
    plt.ylabel('Average Spending')
    plt.title('Top 10 Customers by Average Purchase  ')
    plt.xticks(rotation=45, ha='right')
    plt.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### Top Countries with maximum customers"""

try:
    logging.info("Top 10 countries with maximum customer")
    #customer frequency according to country
    # Order by Purchase Frequency and Limit to Top 10
    customer_volume_country_wise_df = joined_df.groupBy("Country").agg(count("customer_id").alias("Customer_volume_Country_wise"))
    top_10_countries_with_maximum_customer_df = customer_volume_country_wise_df.select("Country","Customer_volume_Country_wise").orderBy(desc("Customer_volume_Country_wise")).limit(10)
    top_10_countries_with_maximum_customer_df.show()

    # Convert to Pandas DataFrame for Visualization
    top_10_countries_with_maximum_customer_pd = top_10_countries_with_maximum_customer_df.toPandas()

    # Plotting the results
    plt.figure(figsize=(12, 8))
    bars = plt.bar(top_10_countries_with_maximum_customer_pd['Country'], top_10_countries_with_maximum_customer_pd['Customer_volume_Country_wise'], color='skyblue')
    plt.xlabel('Country Name')
    plt.ylabel('Customer Volume')
    plt.title('Top 10 Countries By Customers ')
    plt.xticks(rotation=45, ha='right')
    plt.show()

except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)