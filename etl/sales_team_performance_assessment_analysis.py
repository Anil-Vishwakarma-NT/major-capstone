import os
import sys
sys.path.append(os.path.abspath('..'))
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql.functions import col, when, sum, round, month, year, first
import logging

from utils.read import read_csv_files
from utils.visualization import bar_graph, stacked_plot, plot_time_series

# Commented out IPython magic to ensure Python compatibility.
# logger setup
# %cd /spark-data/capstone_crm/utils
# %run 'logger_setup.ipynb'

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
    sys.exit("Exiting due to an unexpected error")

try:
    # Calculate percentage of target achieved
    sales_team_df = sales_team_df.withColumn(
        "Target_Achieved_Percentage",
        round((col("Sales_Achieved") / col("Sales_Target")) * 100, 2))

    # Categorize performance(calculate top performer and under performer )
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
    # top performer
    top_performer_sales_team_df = sales_team_df.orderBy(col("Target_Achieved_Percentage").desc())
    #select name, target achieved and performance category
    top_performer_sales_team_df = top_performer_sales_team_df.select("Name","Target_Achieved_Percentage","Performance_Category").limit(10)
    # show data
    top_performer_sales_team_df.show()
    # Convert the columns from Spark DataFrame to Python lists
    categories = [row['Name'] for row in top_performer_sales_team_df.collect()]
    values = [row['Target_Achieved_Percentage'] for row in top_performer_sales_team_df.collect()]
    # Create a figure and axes object
    fig, ax = plt.subplots(figsize=(10, 6))
    # Call the bar_graph function
    bar_graph(ax=ax, categories=categories, values=values, title="Top 10 Performer in Sales Team", xlabel='Name',
        ylabel="Target Achieved Percentage (%)", color='#B2B0EA', edgecolor='#3C3D99')
    # Display the plot
    plt.show()


except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""#### Analyze under performer"""

try:
    logging.info("Under Performer in Sales Team")
    # calculate under performer
    least_performer_sales_team_df = sales_team_df.orderBy(col("Target_Achieved_Percentage"))
    least_performer_sales_team_df = least_performer_sales_team_df.select("Name","Target_Achieved_Percentage","Performance_Category") \
                                .orderBy(col("Target_Achieved_Percentage")).limit(10)
    #show data
    least_performer_sales_team_df.show()

    # Convert the columns from Spark DataFrame to Python lists
    categories = [row['Name'] for row in least_performer_sales_team_df.collect()]  # Customer name with country in brackets
    values = [row['Target_Achieved_Percentage'] for row in least_performer_sales_team_df.collect()]  # Corresponding average spending
    # Create a figure and axes object
    fig, ax = plt.subplots(figsize=(10, 6))
    # Call the bar_graph function
    bar_graph(ax=ax, categories=categories, values=values, title="Least 10 Performer in Sales Team", xlabel='Name',
        ylabel="Target Achieved Percentage (%)", color='#B2B0EA', edgecolor='#3C3D99')
    # Display the plot
    plt.show()
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
    # plot bar graph
    # Convert the columns from Spark DataFrame to Python lists
    categories = [row['Region'] for row in top_regions_df.collect()]
    values = [row['Total_Sales_Achieved'] for row in top_regions_df.collect()]
    # Create a figure and axes object
    fig, ax = plt.subplots(figsize=(10, 6))
    # Call the bar_graph function
    bar_graph(ax=ax, categories=categories, values=values, title="Top Region On Sales Achieved", xlabel='Region',
        ylabel="Sales Achieved", color='#B2B0EA', edgecolor='#3C3D99')
    # Display the plot
    plt.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""#### Analyze total_sales by Sales Rep and Category"""

try:
    logging.info("Total sales by Sales Rep with Product Category")
    # Join transactions with products to categorize sales by category
    transactions_with_category = transactions_df.join(products_df, "Product_ID")
    # join transaction with category to sales_team_df
    transactions_with_category = transactions_with_category.join(sales_team_df,"Sales_Rep_ID","inner")
    # Calculate total sales by sales rep and category
    category_sales_per_rep = transactions_with_category.groupBy("Sales_Rep_ID", "Category") \
                                    .agg(first("Name").alias("Name"),
                                    sum("Amount").alias("Total_Sales")) \
                                    .orderBy(col("Total_Sales").desc())
    # select Name, Category, Total_sales
    category_sales_per_rep = category_sales_per_rep.select("Name","Category","Total_Sales").orderBy(col("Total_Sales").desc()).limit(10)
    # show
    category_sales_per_rep.show()
    # Convert Spark DataFrame to Pandas DataFrame for plotting
    top_10_pd = category_sales_per_rep.toPandas()

    fig, ax = plt.subplots(figsize=(10, 6))
    # Plotting the data
    stacked_plot(ax,
        data=top_10_pd,
        x_col="Name",
        y_col="Total_Sales",
        hue_col="Category",
        title="Total Sales by Sales Rep with Product Category",
        x_label="Name",
        y_label="Total Sales"
    )
    # plot stacked bar graph
    plt.show()

except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""#### Calculate Sales by sales Rep on year and month"""

try:
    logging.info("Sales By Sales Rep on Year and Month")
    # Extract month from transaction date
    transactions_with_category = transactions_with_category.withColumn("Month", month(col("Date")))
    transactions_with_category = transactions_with_category.withColumn("Year", year(col("Date")))

    # Aggregate sales by month and sales rep
    monthly_sales_pattern = transactions_with_category.groupBy("Year", "Month") \
                                .agg(first("Name").alias("Name"),
                                sum("Amount").alias("Total_Sales"))

    monthly_sales_pattern = monthly_sales_pattern.select("Name","Year","Month","Total_Sales").orderBy(col("Year"),col("Month"))
    monthly_sales_pattern.show()

    categories = [row['Month'] for row in monthly_sales_pattern.collect()]
    values = [[row['Total_Sales'] for row in monthly_sales_pattern.collect()]]
    # Create a figure and axes object
    fig, ax = plt.subplots(figsize=(10, 6))
    # Call the plot_time_series function
    plot_time_series(ax=ax, categories=categories, values=values, title="Monthly Sales Trends",
        x_label="Month", y_label="Total Sales",
        markers=['o'],  # Provide as a list
        linestyles=['-'],  # Provide as a list
        labels=['Revenue']  # Provide as a list
    )
    # Display the plot
    plt.show()

except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)