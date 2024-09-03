import os
import sys
sys.path.append(os.path.abspath('..'))
import pandas as pd
import matplotlib.pyplot as plt

from utils.read import read_csv_files

# Define the path variable
RAW_DATA_PATH = "/spark-data/data/cleaned/"
# read customers data
customers_df = read_csv_files(RAW_DATA_PATH,f"customers.csv")
# read transactions data
transactions_df = read_csv_files(RAW_DATA_PATH,f"transactions.csv")

"""# Analyze customer purchase behavior"""

from pyspark.sql.functions import sum,col,desc,count,round

# get the total spending of the customer
total_spending_df = transactions_df.groupBy("customer_id").agg(sum("Amount").alias("Total_Spending"),count("*").alias("Purchase_Frequency"))
total_spending_df = total_spending_df.withColumn("Avg_spending", round(col("Total_Spending") / col("Purchase_Frequency"), 2))
joined_df = total_spending_df.join(customers_df,'customer_id','left')
joined_df = joined_df.select(col("customer_id"),col("Name"),col("total_Spending"),col("Purchase_Frequency"),col("Avg_spending"),col("Country"))
joined_df.show()

#Get the top N customers based on total spending.
top_ten_customers_df = joined_df.orderBy(desc("total_Spending")).limit(10)
top_ten_customers_df.show()
top_10_customers_pd = top_ten_customers_df.toPandas()

# Plotting the results using Matplotlib - Vertical Bar Chart
plt.figure(figsize=(12, 8))
bars = plt.bar(top_10_customers_pd['Name'], top_10_customers_pd['total_Spending'], color='skyblue')
# Adding Country Names on Top of Each Bar
for bar, country in zip(bars, top_10_customers_pd['Country']):
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval, country, ha='center', va='bottom', fontsize=8, fontweight='bold')
plt.xlabel('Customer Name')
plt.ylabel('Total Spending')
plt.title('Top 10 Customers by Total Spending')
plt.xticks(rotation=45, ha='right')
plt.show()

#calculate purchase frequency per customer

# Order by Purchase Frequency and Limit to Top 10
top_10_frequency_df = joined_df.orderBy(desc("Purchase_Frequency")).limit(10)
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

#calculate purchase frequency per customer

# Order by Purchase Frequency and Limit to Top 10
top_10_avg_spending = joined_df.orderBy(desc("Avg_spending")).limit(10)
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

#customer frequency according to country

# Order by Purchase Frequency and Limit to Top 10
customer_volume_country_wise_df = joined_df.groupBy("Country").agg(count("customer_id").alias("Customer_volume_Country_wise"))
top_10_countries_with_maximum_customer_df = customer_volume_country_wise_df.orderBy(desc("Customer_volume_Country_wise")).limit(20)
# top_10_countries_with_maximum_customer_df.show()

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