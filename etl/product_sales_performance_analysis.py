import os
import sys
sys.path.append(os.path.abspath('..'))
import matplotlib.pyplot as plt
import logging

from utils.read import read_csv_files
from utils.visualization import barh_chart,plot_time_series, plot_pie_chart, bar_graph
from pyspark.sql.functions import sum, count, col,to_date, month, year, first,round,countDistinct

# Commented out IPython magic to ensure Python compatibility.
# logging setup
# %cd /spark-data/capstone_crm/utils
# %run 'logger_setup.ipynb'

# Define the path variable
CLEANED_DATA_PATH = "/spark-data/capstone_crm/data/cleaned/"
logging.info("PRODUCT SALES PERFORMANCE ANALYSIS.......")

try:
    logging.info("reading csv files")
    # read products data
    products_df = read_csv_files(f"{CLEANED_DATA_PATH}products.csv")
    # read transactions data
    transactions_df = read_csv_files(f"{CLEANED_DATA_PATH}transactions.csv")
except Exception as e:
    logging.error("Error in loading files :{e}", exc_info=True)

"""# Product Sales Performance Analysis

###  Top products on revenue
"""

try:
    # Calculate total revenue for each product
    logging.info("Top 10 products on total revenue")
    # join transaction df to product df
    product_sales_df = transactions_df.join(products_df,"Product_ID", "inner")
    products_revenue_df = product_sales_df.groupBy("Product_ID").agg(
        first("Product_Name").alias("Product_Name"),
        first("Price").alias("Price"),
        sum("Amount").alias("Total_Revenue"),
        count("*").alias("Sales_Volume")
    )
    #select Product Name, Total Revenue
    revenue_by_products = products_revenue_df.select("Product_Name","Total_Revenue").orderBy(col("Total_Revenue").desc()).limit(10)
    revenue_by_products.show()
    # Convert the columns from Spark DataFrame to Python lists
    categories = [row['Product_Name'] for row in revenue_by_products.collect()]
    values = [row['Total_Revenue'] for row in revenue_by_products.collect()]
    # Create a figure and axes object
    fig, ax = plt.subplots(figsize=(10, 6))
    # Call the bar_graph function
    barh_chart(ax=ax, categories=categories, values=values, title="Top 10 Products by Total Revenue",xlabel="Total Revenue",
                ylabel="Product", color='#B2B0EA', edgecolor='#3C3D99')

    # Display the plot
    plt.show()

except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### Top product on Sales Volume"""

try:
    # Calculate sales volume for each product
    logging.info("Top 10 products on sales volume")
    # select product name and sales volume
    product_volume_df = products_revenue_df.select("Product_Name","Sales_Volume")
    product_volume_df = product_volume_df.orderBy(col("Sales_Volume").desc()).limit(10)
    product_volume_df.show()
    # Convert the columns from Spark DataFrame to Python lists
    categories = [row['Product_Name'] for row in product_volume_df.collect()]
    values = [row['Sales_Volume'] for row in product_volume_df.collect()]
    # Create a figure and axes object
    fig, ax = plt.subplots(figsize=(10, 6))
    # Call the bar_graph function
    barh_chart(ax=ax, categories=categories, values=values, title="Top 10 Products by Sales Volume", xlabel="Product Volume",
        ylabel="Product", color='#B2B0EA', edgecolor='#3C3D99')
    # Display the plot
    plt.show()

except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)



"""### Category wise total revenue"""

try:
    # Calculate total revenue by category
    logging.info("Category wise total revenue")
    # get revenue grouping by category
    category_performance_df = product_sales_df.groupBy("Category").agg(
        first("Product_Name").alias("Product_Name"),
        sum("Amount").alias("Category_Total_Revenue"),
        count("*").alias("Category_Sales_Volume"),
        countDistinct("Customer_ID").alias("distinct_customer")
    )
    # select product name, category and revenue
    category_revenue_df = category_performance_df.select("Category","Product_Name","Category_Total_Revenue").orderBy(col("Category_Total_Revenue").desc())
    category_revenue_df.show()
    # plot pie chart
    # Extract lists for plotting
    categories = [row['Category'] for row in category_revenue_df.collect()]
    revenues = [row['Category_Total_Revenue'] for row in category_revenue_df.collect()]
    # Define the explode values (optional, can be customized)
    explode = [0.05 if i == 0 else 0 for i in range(len(categories))]  # Emphasizing the highest revenue category
    # Create a figure and axes object
    fig, ax = plt.subplots(figsize=(6, 6))
    # Call the generalized pie chart function
    plot_pie_chart(ax,sizes=revenues,labels=categories,explode=explode,
            title="Revenue Contribution by Category")
    plt.show()

except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### Calculate sales volume by category"""

try:
        # Calculate sales volume by category
        logging.info("Sales volume by Category")
        # select category, product name, volume
        category_volume_df = category_performance_df.select("Category","Product_Name","Category_Sales_Volume") \
                        .orderBy(col("Category_Sales_Volume").desc())
        # show data
        category_volume_df.show()
        # Extract lists for plotting
        categories = [row['Category'] for row in category_volume_df.collect()]
        revenues = [row['Category_Sales_Volume'] for row in category_volume_df.collect()]
        # Define the explode values (optional, can be customized)
        explode = [0.05 if i == 0 else 0 for i in range(len(categories))]  # Emphasizing the highest revenue category
        # Create a figure and axes object
        fig, ax = plt.subplots(figsize=(6, 6))
        # Call the generalized pie chart function
        plot_pie_chart(ax,sizes=revenues,labels=categories,explode=explode,
                title="Sales volume by Category")
        plt.show()
except Exception as e:
        logging.error("invalid operation performed :{e}", exc_info=True)
        raise

"""#### Identify Distinct customers on category"""

try:
        # Calculate customers on category
        logging.info("Identify Customers By Category")
        # select category, product name, volume
        customers_by_category_df = category_performance_df.select("Category","distinct_customer") \
                        .orderBy(col("Category_Sales_Volume").desc())
        # show data
        customers_by_category_df.show()
        # Extract lists for plotting
        categories = [row['Category'] for row in customers_by_category_df.collect()]
        values = [row['distinct_customer'] for row in customers_by_category_df.collect()]
        # Create a figure and axes object
        fig, ax = plt.subplots(figsize=(10, 6))
        # Call the bar_graph function
        bar_graph(ax=ax, categories=categories, values=values, title="Identify Customers By Category", xlabel="Category",
                ylabel="Distinct Customers", color='#B2B0EA', edgecolor='#3C3D99')
        # Display the plot
        plt.show()
except Exception as e:
        logging.error("invalid operation performed :{e}", exc_info=True)

"""### Identify least-selling products on total revenue"""

try:
    # Identify least-selling products by total revenue
    logging.info("Least 10 selling products on total revenue")
    # get least 10 products by revenue
    least_selling_products_df = products_revenue_df.orderBy(col("Total_Revenue")).limit(10)
    # show data
    least_selling_products_df.show()
    # Convert the columns from Spark DataFrame to Python lists
    categories = [row['Product_Name'] for row in least_selling_products_df.collect()]
    values = [row['Total_Revenue'] for row in least_selling_products_df.collect()]
    # Create a figure and axes object
    fig, ax = plt.subplots(figsize=(10, 6))
    # Call the bar_graph function
    barh_chart(ax=ax, categories=categories, values=values, title="Least 10 Products by Total Revenue",
        xlabel="Total Revenue", ylabel="Product", color='#B2B0EA', edgecolor='#3C3D99')
    # Display the plot
    plt.show()

except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### Identify least-selling products on sales volume"""

try:
    # Identify least-selling products by sales volume
    logging.info("least 10 products on sales volume")
    # get least 10 products by volume
    least_selling_products_by_volume_df = products_revenue_df.orderBy(col("Sales_Volume")).limit(10)
    # show data
    least_selling_products_by_volume_df.show()
    # Convert the columns from Spark DataFrame to Python lists
    categories = [row['Product_Name'] for row in least_selling_products_by_volume_df.collect()]
    values = [row['Sales_Volume'] for row in least_selling_products_by_volume_df.collect()]
    # Create a figure and axes object
    fig, ax = plt.subplots(figsize=(10, 6))
    # Call the bar_graph function
    barh_chart(ax=ax, categories=categories, values=values, title="Least 10 Products by Sales Volume", xlabel="Sales Volume",
        ylabel="Product", color='#B2B0EA', edgecolor='#3C3D99')
    # Display the plot
    plt.show()

except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### Calculate the percentage contribution of product to total revenue"""

try:
    # Calculate the percentage contribution of each product to total revenue
    logging.info("Top 10 products contributing to total revenue")
    #Calculate total revenue
    total_revenue = products_revenue_df.agg(sum("Total_Revenue")).first()[0]
    #calculate percentage as a new column
    product_contribution_df = products_revenue_df.withColumn("Revenue_Contribution_Percent",round((col("Total_Revenue") / total_revenue) * 100, 2))
    #select name and revenue percentage
    product_contribution_df = product_contribution_df.select("Product_Name", "Revenue_Contribution_Percent").orderBy(
        col("Revenue_Contribution_Percent").desc()).limit(10)
    product_contribution_df.show()

    # Convert the columns from Spark DataFrame to Python lists
    categories = [row['Product_Name'] for row in product_contribution_df.collect()]
    values = [row['Revenue_Contribution_Percent'] for row in product_contribution_df.collect()]
    # Create a figure and axes object
    fig, ax = plt.subplots(figsize=(10, 6))

    # Call the bar_graph function
    barh_chart(ax=ax, categories=categories, values=values, title="Top 10 Products by Revenue Contribution", xlabel="Revenue Contribution (%)",
        ylabel="Product", color='#B2B0EA', edgecolor='#3C3D99')
    # Display the plot
    plt.show()

except Exception as e:
    logging.error(f"Invalid operation performed: {e}", exc_info=True)

"""### Monthly sales patterns"""

try:
    # Monthly sales patterns
    logging.info("Monthly sales patterns")
    transactions_df = transactions_df.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))
    # Analyze sales trends by month and year
    monthly_sales_df = transactions_df.groupBy(year("Date").alias("Year"), month("Date").alias("Month")).agg(
        sum("Amount").alias("Monthly_Revenue"),
        count("*").alias("Monthly_Sales_Volume")
    ).orderBy("Year", "Month")
    monthly_sales_df.show(10)

except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

try:
    logging.info("Monthly Sales Patterns on Revenue")
    # Convert the columns from Spark DataFrame to Python lists
    categories = [row['Month'] for row in monthly_sales_df.collect()]
    values = [[row['Monthly_Revenue'] for row in monthly_sales_df.collect()]]
    # Create a figure and axes object
    fig, ax = plt.subplots(figsize=(10, 6))
    # Call the plot_time_series function
    plot_time_series(ax=ax, categories=categories, values=values, title="Monthly Sales Trends on Revenue",
        x_label="Month", y_label="Value",
        markers=['o'],  # Provide as a list
        linestyles=['-'],  # Provide as a list
        labels=['Revenue']  # Provide as a list
    )
    # Display the plot
    plt.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

try:
    logging.info("Monthly Sales Trends On Sales Volume")
    # Convert the columns from Spark DataFrame to Python lists
    categories = [row['Month'] for row in monthly_sales_df.collect()]
    values = [[row['Monthly_Sales_Volume'] for row in monthly_sales_df.collect()]]
    # Create a figure and axes object
    fig, ax = plt.subplots(figsize=(10, 6))
    # Call the plot_time_series function
    plot_time_series(ax=ax, categories=categories, values=values,   title="Monthly Sales Trends", x_label="Month",
        y_label="Value",  markers=['x'], linestyles=['--'],  labels=['Sales Volume'] )
    # Display the plot
    plt.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)