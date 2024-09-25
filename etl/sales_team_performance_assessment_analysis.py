import os
import sys
sys.path.append(os.path.abspath('..'))
import matplotlib.pyplot as plt
from pyspark.sql.functions import col, when, sum, round, month, year, first
import logging
from utils.read_write import read_csv_files
from utils.visualization import bar_graph, stacked_plot, plot_time_series
from utils.logger_setup import setup_logging
# import the path variable
from utils.get_config_data import CLEANED_DATA_DIR

logging.info("SALES TEAM PERFORMANCE ANALYSIS......................")
# Sales Team Performance Assessment Analysis
try:  
    logging.info("loading csv files")
    # read customers data
    sales_team_df = read_csv_files(f"{CLEANED_DATA_DIR}/sales_team.csv")
    # read transactions data
    transactions_df = read_csv_files(f"{CLEANED_DATA_DIR}/transactions.csv")
    # read products data
    products_df = read_csv_files(f"{CLEANED_DATA_DIR}/products.csv")
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)
    sys.exit("Exiting due to an unexpected error")     
try:    
    # Calculate percentage of target achieved 
    sales_team_df = sales_team_df.withColumn("Target_Achieved_Percentage",
                             round((col("Sales_Achieved") / col("Sales_Target")) * 100, 2))

except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)
### Analyze Top Performer 
try:
    logging.info("Top Performer In Sales Team")   
    # top performer 
    top_performer_sales_team_df = sales_team_df.orderBy(col("Target_Achieved_Percentage").desc())
    #select name, target achieved and performance category
    top_performer_sales_team_df = top_performer_sales_team_df.select("Name","Target_Achieved_Percentage") \
        .limit(10)
    # show dataframe
    top_performer_sales_team_df.show()
    # Convert the columns from Spark DataFrame to Python lists
    categories = [row['Name'] for row in top_performer_sales_team_df.collect()]  
    values = [row['Target_Achieved_Percentage'] for row in top_performer_sales_team_df.collect()] 
    # Create a figure and axes object
    fig, ax = plt.subplots(figsize=(10, 6))
    # Call the bar_graph function
    bar_graph(ax=ax, 
            categories=categories, 
            values=values, 
            title="Top Performer In Sales Team", 
            xlabel='Name', 
            ylabel="Target Achieved Percentage (%)", 
            color='#B2B0EA', 
            edgecolor='#3C3D99'
            )
    # Display the plot
    plt.show()
    
    
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)    
#### Analyze Under Performer
try:
    logging.info("Under Performer In Sales Team")   
    # calculate under performer
    least_performer_sales_team_df = sales_team_df.orderBy(col("Target_Achieved_Percentage"))
    least_performer_sales_team_df = least_performer_sales_team_df.select("Name","Target_Achieved_Percentage") \
                                .orderBy(col("Target_Achieved_Percentage")) \
                                .limit(10)
    #show data
    least_performer_sales_team_df.show()
    
    # Convert the columns from Spark DataFrame to Python lists
    categories = [row['Name'] for row in least_performer_sales_team_df.collect()]  
    values = [row['Target_Achieved_Percentage'] for row in least_performer_sales_team_df.collect()]   
    # Create a figure and axes object
    fig, ax = plt.subplots(figsize=(10, 6))
    # Call the bar_graph function
    bar_graph(ax=ax, 
            categories=categories, 
            values=values, 
            title="Under Performer In Sales Team", 
            xlabel='Name', 
            ylabel="Target Achieved Percentage (%)", 
            color='#B2B0EA', 
            edgecolor='#3C3D99'
            )
    # Display the plot
    plt.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)    
#### Top Region On Sales Achieved
try:
    logging.info("Top Region On Sales Achieved")    
    # calculate Top regions on Sales
    top_regions_df  = sales_team_df.groupBy("Region") \
                    .agg(sum("Sales_Achieved").alias("Total_Sales_Achieved"),
                    first("Sales_Target").alias("Sales_Target"))
    
    top_regions_df = top_regions_df.orderBy(col("Total_Sales_Achieved").desc()).limit(10)
    top_regions_df.show() 

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
#### Analyze Total Sales By Sales Rep And Category
try:
    logging.info("Total Sales By Sales Rep With Product Category")    
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
    category_sales_per_rep = category_sales_per_rep.select("Name","Category","Total_Sales") \
            .orderBy(col("Total_Sales").desc()) \
            .limit(10)
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
        title="Total Sales by Sales Rep With Product Category",
        x_label="Name",
        y_label="Total Sales"
    )
    # plot stacked bar graph
    plt.show()
    
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)    
#### Calculate Sales By Sales Rep On Year And Month
try:
    logging.info("Sales By Sales Rep On Year And Month")    
    # Extract month from transaction date
    transactions_with_category = transactions_with_category.withColumn("Month", month(col("Date")))
    transactions_with_category = transactions_with_category.withColumn("Year", year(col("Date")))

    # Aggregate sales by month and sales rep
    monthly_sales_pattern = transactions_with_category.groupBy("Year", "Month") \
                                .agg(first("Name").alias("Name"),
                                sum("Amount").alias("Total_Sales"))

    monthly_sales_pattern = monthly_sales_pattern.select("Name","Year","Month","Total_Sales") \
        .orderBy(col("Year"),col("Month"))
    
    # show dataframe    
    monthly_sales_pattern.show()
    
    categories = [row['Month'] for row in monthly_sales_pattern.collect()]
    values = [row['Total_Sales'] for row in monthly_sales_pattern.collect()]  
    # Create a figure and axes object
    fig, ax = plt.subplots(figsize=(10, 6))
    # Call the plot_time_series function
    plot_time_series(ax=ax, 
                    categories=categories, 
                    values=values, 
                    title="Monthly Sales Trends", 
                    x_label="Month(2024)", 
                    y_label="Total Sales",  
                    marker='o',
                    linestyle='-', 
                    label='Revenue' 
                    )
    # Display the plot
    plt.show()

except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)    
