import os
import sys
sys.path.append(os.path.abspath('..'))
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql.functions import col, when, sum, round, month, year, first
import logging

from utils.read import read_csv_files

# Define the path variable
CLEANED_DATA_PATH = "/spark-data/capstone_crm/data/cleaned/"

"""# Sales Team Performance Assessment Analysis"""

try:
    logging.info("loading csv files")
    # read customers data
    sales_team_df = read_csv_files(f"{CLEANED_DATA_PATH}sales_team.csv")
    # read transactions data
    transactions_df = read_csv_files(f"{CLEANED_DATA_PATH}transactions.csv")
    # read products data
    products_df = read_csv_files(f"{CLEANED_DATA_PATH}products.csv")
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

try:
    # Calculate percentage of target achieved (calculate top performer and under performer )
    sales_team_df = sales_team_df.withColumn(
        "Target_Achieved_Percentage",
        round((col("Sales_Achieved") / col("Sales_Target")) * 100, 2))

    # Categorize performance
    sales_team_df = sales_team_df.withColumn(
        "Performance_Category",
        when(col("Target_Achieved_Percentage") >= 100, "Top Performer")
        .when(col("Target_Achieved_Percentage") >= 80, "Average Performer")
        .otherwise("Under Performer")
    )

except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### Analyze top performer"""

try:
    logging.info("Top 10 Performer in Sales Team")
    #Calculate top performer
    top_performer_sales_team_df = sales_team_df.orderBy(col("Target_Achieved_Percentage").desc())
    top_performer_sales_team_df = top_performer_sales_team_df.select("Name","Target_Achieved_Percentage","Performance_Category").limit(10)
    top_performer_sales_team_df.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""#### Analyze under performer"""

try:
    logging.info("Under Performer in Sales Team")
    # calculate under performer
    least_performer_sales_team_df = sales_team_df.orderBy(col("Target_Achieved_Percentage"))
    least_performer_sales_team_df = least_performer_sales_team_df.select("Name","Target_Achieved_Percentage","Performance_Category") \
                                .orderBy(col("Target_Achieved_Percentage")).limit(10)
    least_performer_sales_team_df.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""#### Top Region On Sales Achieved"""

try:
    logging.info("Top Region On Sales Achieved")
    # calculate Top regions on Sales
    top_regions_df  = sales_team_df.groupBy("Region") \
                    .agg(sum("Sales_Achieved").alias("Total_Sales_Achieved"),first("Sales_Target").alias("Sales_Target"))

    top_regions_df = top_regions_df.orderBy(col("Total_Sales_Achieved").desc()).limit(10)
    top_regions_df.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""#### Analyze total_sales by Sales Rep and Category"""

try:
    logging.info("Total sales by Sales Rep and Category")
    # Join transactions with products to categorize sales by category
    transactions_with_category = transactions_df.join(products_df, "Product_ID")
    # join transaction with category to sales_team_df
    transactions_with_category = transactions_with_category.join(sales_team_df,"Sales_Rep_ID","inner")
    # Calculate total sales by sales rep and category
    category_sales_per_rep = transactions_with_category.groupBy("Sales_Rep_ID", "Category") \
                                    .agg(first("Name").alias("Name"),
                                    sum("Amount").alias("Total_Sales")) \
                                    .orderBy(col("Total_Sales").desc())

    category_sales_per_rep = category_sales_per_rep.select("Name","Category","Total_Sales").orderBy(col("Total_Sales").desc()).limit(10)
    category_sales_per_rep.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""#### Calculate Sales by sales Rep on year and month"""

try:
    logging.info("Sales By Sales Rep on Year and Month")
    # Extract month from transaction date
    transactions_with_category = transactions_with_category.withColumn("Month", month(col("Date")))
    transactions_with_category = transactions_with_category.withColumn("Year", year(col("Date")))

    # Aggregate sales by month and sales rep
    monthly_sales_per_rep = transactions_with_category.groupBy("Sales_Rep_ID", "Year", "Month") \
                                .agg(first("Name").alias("Name"),
                                sum("Amount").alias("Total_Sales"))

    monthly_sales_per_rep = monthly_sales_per_rep.select("Name","Year","Month","Total_Sales").orderBy(col("Total_Sales").desc()).limit(10)
    monthly_sales_per_rep.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

try:
    logging.info()
    # Convert to Pandas for visualization
    final_performance_pd = final_performance_df.limit(20).toPandas()

    # Plot: Target Achieved Percentage
    plt.figure(figsize=(10, 6))
    plt.barh(final_performance_pd['Name'], final_performance_pd['Target_Achieved_Percentage'], color='skyblue')
    plt.xlabel('Target Achieved (%)')
    plt.title('Sales Target Achieved by Each Sales Representative')
    plt.grid(True)
    plt.show()

    # Plot: Revenue Percentage of Target
    plt.figure(figsize=(10, 6))
    plt.barh(final_performance_pd['Name'], final_performance_pd['Revenue_Percentage_Target'], color='orange')
    plt.xlabel('Revenue Percentage of Target (%)')
    plt.title('Revenue Generated as Percentage of Sales Target')
    plt.grid(True)
    plt.show()

    # Pie Chart: Performance Categories
    performance_categories = final_performance_pd['Performance_Category'].value_counts()
    plt.figure(figsize=(8, 8))
    plt.pie(performance_categories, labels=performance_categories.index, autopct='%1.1f%%', startangle=140, colors=['green', 'yellow', 'red'])
    plt.title('Distribution of Sales Performance Categories')
    plt.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)