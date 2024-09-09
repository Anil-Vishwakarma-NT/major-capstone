import os
import sys
sys.path.append(os.path.abspath('..'))
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging

from pyspark.sql.functions import col, when, count, round, year, month
from utils.read import read_csv_files

# Define the path variable
CLEANED_DATA_PATH = "/spark-data/capstone_crm/data/cleaned/"

"""# Customer Interaction effectiveness Analysis"""

try:
    logging.info("reading csv files")
    # read customers data
    customers_df = read_csv_files(f"{CLEANED_DATA_PATH}customers.csv")
    # read transactions data
    interactions_df = read_csv_files(f"{CLEANED_DATA_PATH}interactions.csv")
except Exception as e:
    logging.error("Error in loading files :{e}", exc_info=True)

"""###  resolution rates for each interaction type"""

try:
    logging.info("Resolution Rate For each Interaction Type")
    # Calculate resolution rates for each interaction type
    resolution_rates_df = interactions_df.groupBy("Interaction_Type").agg(
        count("Issue_Resolved").alias("Total_Interactions"),
        count(when(col("Issue_Resolved") == "Yes", True)).alias("Resolved_Issues")
    ).withColumn(
        "Resolution_Rate", round(col("Resolved_Issues") / col("Total_Interactions") * 100, 2)
    )

    resolution_rates_df.show()

    # Convert to Pandas DataFrame for plotting
    resolution_rates_pd = resolution_rates_df.toPandas()

    # Plot

    plt.figure(figsize=(8, 8))
    plt.pie(resolution_rates_pd['Resolution_Rate'], labels=resolution_rates_pd['Interaction_Type'],
            autopct='%1.1f%%', startangle=140, colors=plt.cm.Paired.colors)
    plt.title('Revenue Contribution by Category')
    plt.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

try:
    # Join customers with interactions to analyze preferences
    customer_interaction_df = interactions_df.join(customers_df, "Customer_ID")
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### Calculate preference for each interaction type by country"""

try:
    logging.info("Top 10 reference for each interaction type by country")
    # Calculate preference for each interaction type by country
    country_preference_interaction_type_df = customer_interaction_df.groupBy("Country", "Interaction_Type").agg(
        count("*").alias("Interaction_Count")
    ).orderBy("Interaction_Count", ascending=False)

    country_preference_interaction_type_df = country_preference_interaction_type_df.orderBy(col("Interaction_Count").desc()).limit(10)
    country_preference_interaction_type_df.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### Calculate countries with highest Interaction count"""

try:
    logging.info("top 10 countries with highest interaction count")
    # Calculate countries with interaction count
    country_preference_df = customer_interaction_df.groupBy("Country").agg(
        count("*").alias("Total_Interaction_Count")
    ).orderBy("Total_Interaction_Count", ascending=False)
    country_preference_df = country_preference_df.limit(10)
    country_preference_df.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### Calculate resolution rates and issue Resolved by country"""

try:
    logging.info("Countries where resolution rate is low")
    # Calculate resolution rates by country
    country_resolution_df = customer_interaction_df.groupBy("Country").agg(
        count("Issue_Resolved").alias("Total_Interactions"),
        count(when(col("Issue_Resolved") == "Yes", True)).alias("Resolved_Issues")
    ).withColumn(
        "Resolution_Rate", round(col("Resolved_Issues") / col("Total_Interactions") * 100, 2)
    )

    country_resolution_df = country_resolution_df.orderBy(col("Resolution_Rate")).limit(10)
    country_resolution_df.show()


except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### calculate the issue with respect to particular customer and interaction type"""

try:
    logging.info("calculate Top 10 customers with interaction type who raised most interaction")
    #calculate the issue with respect to particular customer and interaction type
    customer_interaction_preference = customer_interaction_df.groupBy("Interaction_Type","Name").agg(
        count("*").alias("issue_raised")
    ).orderBy("issue_raised",ascending = False)

    customer_interaction_preference=customer_interaction_preference.select("Name","Interaction_Type","issue_raised").orderBy(col("issue_raised").desc()).limit(10)
    customer_interaction_preference.show()

except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### Calculate resolution rates by year and month"""

try:
    logging.info("Calculate resolution rates by year and month")
    # Extract year and month from interaction date
    interactions_df = interactions_df.withColumn("Year", year("Interaction_Date"))
    interactions_df = interactions_df.withColumn("Month", month("Interaction_Date"))

    # Calculate resolution rates by year and month
    monthly_resolution_df = interactions_df.groupBy("Year", "Month", "Interaction_Type").agg(
        count("Issue_Resolved").alias("Total_Interactions"),
        count(when(col("Issue_Resolved") == "Yes", True)).alias("Resolved_Issues")
    ).withColumn(
        "Resolution_Rate", round(col("Resolved_Issues") / col("Total_Interactions") * 100, 2)
    ).orderBy("Year", "Month")

    monthly_resolution_df.show()

    # Convert to Pandas DataFrame for plotting
    monthly_resolution_pd = monthly_resolution_df.toPandas()

    # Plot using Matplotlib
    plt.figure(figsize=(12, 8))
    sns.lineplot(data=monthly_resolution_pd, x='Month', y='Resolution_Rate', hue='Year', style='Interaction_Type', markers=True)
    plt.xlabel('Month')
    plt.ylabel('Resolution Rate (%)')
    plt.title('Monthly Resolution Rates by Interaction Type')
    plt.show()

except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)