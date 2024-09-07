import os
import sys
sys.path.append(os.path.abspath('..'))
import pandas as pd
import matplotlib.pyplot as plt
import logging

from utils.read import read_csv_files
from pyspark.sql.functions import sum, count, col,to_date, month, year, first

# Define the path variable
CLEANED_DATA_PATH = "/spark-data/capstone_crm/data/cleaned/"
logging.info("PRODUCT SALES PERFORMANCE ANALYSIS.......")

try:
    logging.warning("reading csv files")
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
    product_sales_df = transactions_df.join(products_df,"Product_ID", "inner")
    products_revenue_df = product_sales_df.groupBy("Product_ID").agg(
        first("Product_Name").alias("Product_Name"),
        first("Price").alias("Price"),
        sum("Amount").alias("Total_Revenue"),
        count("*").alias("Sales_Volume")
    )
    products_revenue_df.select("Product_Name","Total_Revenue").orderBy("Total_Revenue",ascending = False).show(10)
    products_revenue_pd = products_revenue_df.orderBy("Total_Revenue",ascending = False).limit(10).toPandas()

    # Plot
    plt.figure(figsize=(10, 6))
    plt.barh(products_revenue_pd['Product_Name'], products_revenue_pd['Total_Revenue'], color='green')
    plt.xlabel('Total Revenue')
    plt.ylabel('Product')
    plt.title('Top 10 Products by Total Revenue')
    plt.gca().invert_yaxis()  # Invert y-axis to have the highest revenue at the top
    plt.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### Top product on Sales Volume"""

try:
    # Calculate total revenue and sales volume for each product
    logging.info("Top 10 products on sales volume")
    product_volume_df = products_revenue_df.select("Product_Name","Sales_Volume")
    product_volume_df.orderBy(col("SAles_Volume").desc()).show(10)

    products_volume_pd = product_volume_df.orderBy("Sales_Volume",ascending = False).limit(10).toPandas()

    # Plot
    plt.figure(figsize=(10, 6))
    plt.barh(products_volume_pd['Product_Name'], products_volume_pd['Sales_Volume'], color='green')
    plt.xlabel('Total Revenue')
    plt.ylabel('Product')
    plt.title('Top 10 Products by Total Revenue')
    plt.gca().invert_yaxis()  # Invert y-axis to have the highest revenue at the top
    plt.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### Category wise total revenue"""

try:
    # Calculate total revenue by category
    logging.info("Category wise total revenue")
    category_performance_df = product_sales_df.groupBy("Category").agg(
        first("Product_Name").alias("Product_Name"),
        sum("Amount").alias("Category_Total_Revenue"),
        count("*").alias("Category_Sales_Volume")
    )
    category_revenue_df = category_performance_df.select("Category","Product_Name","Category_Total_Revenue").orderBy("Category_Total_Revenue",ascending=False)
    category_revenue_df.show()

    # Convert category performance data to Pandas DataFrame
    category_revenue_pd = category_revenue_df.toPandas()

    # Plot
    plt.figure(figsize=(8, 8))
    plt.pie(category_revenue_pd['Category_Total_Revenue'], labels=category_revenue_pd['Category'],
            autopct='%1.1f%%', startangle=140, colors=plt.cm.Paired.colors)
    plt.title('Revenue Contribution by Category')
    plt.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### Calculate sales volume by category"""

try:
        # Calculate sales volume by category
        logging.info("Sales volume by Category")
        category_volume_df = category_performance_df.select("Category","Product_Name","Category_Sales_Volume") \
                             .orderBy(col("Category_Sales_Volume").desc())
        category_volume_df.show()

        # Convert category performance data to Pandas DataFrame
        category_volume_pd = category_volume_df.toPandas()

        # Plot
        plt.figure(figsize=(8, 8))
        plt.pie(category_volume_pd['Category_Sales_Volume'], labels=category_volume_pd['Category'],
                autopct='%1.1f%%', startangle=140, colors=plt.cm.Paired.colors)
        plt.title('Revenue Contribution by Category')
        plt.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### Identify least-selling products on total revenue"""

try:
    # Identify least-selling products by total revenue
    logging.info("Least 10 selling products on total revenue")
    least_selling_products_df = products_revenue_df.orderBy(col("Total_Revenue"))

    # Convert the least 10 products by revenue to Pandas DataFrame
    least_selling_products_pd = least_selling_products_df.limit(10).toPandas()

    # Plot
    plt.figure(figsize=(10, 6))
    plt.barh(least_selling_products_pd['Product_Name'], least_selling_products_pd['Total_Revenue'], color='skyblue')
    plt.xlabel('Total Revenue')
    plt.ylabel('Product')
    plt.title('least 10 Products by Total Revenue')
    plt.gca().invert_yaxis()  # Invert y-axis to have the highest revenue at the top
    plt.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### Identify least-selling products on sales volume"""

try:
    # Identify least-selling products by sales volume
    logging.info("least 10 products on sales volume")
    least_selling_products_by_volume_df = products_revenue_df.orderBy(col("Sales_Volume"))

    # Convert the top 10 products by revenue to Pandas DataFrame
    least_selling_products_by_volume_pd = least_selling_products_by_volume_df.limit(10).toPandas()

    # Plot
    plt.figure(figsize=(10, 6))
    plt.barh(least_selling_products_by_volume_pd['Product_Name'], least_selling_products_by_volume_pd['Sales_Volume'], color='lightgreen')
    plt.xlabel('Sales Volume')
    plt.ylabel('Product')
    plt.title('least 10 Products by Sales Volume')
    plt.gca().invert_yaxis()  # Invert y-axis to have the highest revenue at the top
    plt.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### correlation between price and sales volume"""

try:
    # Analyze the correlation between price and sales volume
    logging.info("Correlation between price and sales volume")
    price_sensitivity_df = products_revenue_df.withColumn("Price", col("Price"))
    # Calculate correlation between price and sales volume
    price_volume_correlation = price_sensitivity_df.stat.corr("Price", "Sales_Volume")

    # Convert the product sales data to Pandas DataFrame
    price_sensitivity_pd = price_sensitivity_df.toPandas()

    # Plot
    plt.figure(figsize=(10, 6))
    plt.scatter(price_sensitivity_pd['Price'], price_sensitivity_pd['Sales_Volume'], alpha=0.6, color='purple')
    plt.xlabel('Price')
    plt.ylabel('Sales Volume')
    plt.title('Price vs Sales Volume')
    plt.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### Calculate the percentage contribution of product to total revenue"""

try:
    # Calculate the percentage contribution of each product to total revenue
    logging.info("Top 10 product who contribute in total revenue")
    total_revenue = products_revenue_df.agg(sum("Total_Revenue")).first()[0]

    product_contribution_df = products_revenue_df.withColumn(
        "Revenue_Contribution_Percent",
        (col("Total_Revenue") / total_revenue) * 100
    )
    product_contribution_df = product_contribution_df.select("Product_Name","Revenue_Contribution_Percent").orderBy(col("Revenue_Contribution_Percent").desc())
    product_contribution_df.show(10)
    # Convert to Pandas DataFrame
    # Top products which contribute in total revenue
    product_contribution_pd = product_contribution_df.limit(10).toPandas()

    # Plot
    plt.figure(figsize=(10, 6))
    plt.barh(product_contribution_pd['Product_Name'], product_contribution_pd['Revenue_Contribution_Percent'], color='salmon')
    plt.xlabel('Revenue Contribution (%)')
    plt.ylabel('Product')
    plt.title('Top 10 Products by Revenue Contribution')
    plt.gca().invert_yaxis()
    plt.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

"""### Monthly sales patterns"""

try:
    # Monthly sales patterns
    # Convert the Date column to a proper date type
    logging.info("Monthly sales patterns")
    transactions_df = transactions_df.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))

    # Analyze sales trends by month and year
    monthly_sales_df = transactions_df.groupBy(year("Date").alias("Year"), month("Date").alias("Month")).agg(
        sum("Amount").alias("Monthly_Revenue"),
        count("*").alias("Monthly_Sales_Volume")
    ).orderBy("Year", "Month")

    # Convert the monthly sales data to Pandas DataFrame
    monthly_sales_pd = monthly_sales_df.toPandas()

    # Plot
    plt.figure(figsize=(12, 6))
    plt.plot(monthly_sales_pd['Month'], monthly_sales_pd['Monthly_Revenue'], marker='o', label='Revenue')
    plt.plot(monthly_sales_pd['Month'], monthly_sales_pd['Monthly_Sales_Volume'], marker='x', label='Sales Volume', linestyle='--')
    plt.xlabel('Month')
    plt.ylabel('Value')
    plt.title('Monthly Sales Trends')
    plt.legend()
    plt.grid(True)
    plt.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)

