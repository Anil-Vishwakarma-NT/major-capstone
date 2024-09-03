import os
import sys
sys.path.append(os.path.abspath('..'))
import pandas as pd
import matplotlib.pyplot as plt

from utils.read import read_csv_files
from pyspark.sql.functions import sum, count, col,to_date, month, year

# read products data
products_df = read_csv_files("/spark-data/capstone_crm/data/cleaned/products.csv")
# read transactions data
transactions_df = read_csv_files("/spark-data/capstone_crm/data/cleaned/transactions.csv")

"""# Product Sales Performance Analysis"""

# Calculate total revenue and sales volume for each product
product_sales_df = transactions_df.groupBy("Product_ID").agg(
    sum("Amount").alias("Total_Revenue"),
    count("*").alias("Sales_Volume")
)
product_sales_df.show(5)

# Join transactions with products data to get category and price information
product_sales_df = product_sales_df.join(products_df, "Product_ID", "inner")
product_sales_df.show(5)

# Calculate total revenue and sales volume by category
category_performance_df = product_sales_df.groupBy("Category").agg(
    sum("Total_Revenue").alias("Category_Total_Revenue"),
    sum("Sales_Volume").alias("Category_Sales_Volume")
)
# Convert category performance data to Pandas DataFrame
category_performance_pd = category_performance_df.toPandas()

# Plot
plt.figure(figsize=(8, 8))
plt.pie(category_performance_pd['Category_Total_Revenue'], labels=category_performance_pd['Category'],
        autopct='%1.1f%%', startangle=140, colors=plt.cm.Paired.colors)
plt.title('Revenue Contribution by Category')
plt.show()

# Identify top-selling products by total revenue
top_selling_products_df = product_sales_df.orderBy(col("Total_Revenue").desc())

# Convert the top 20 products by revenue to Pandas DataFrame
top_selling_products_pd = top_selling_products_df.limit(20).toPandas()

# Plot
plt.figure(figsize=(10, 6))
plt.barh(top_selling_products_pd['Product_Name'], top_selling_products_pd['Total_Revenue'], color='skyblue')
plt.xlabel('Total Revenue')
plt.ylabel('Product')
plt.title('Top 10 Products by Total Revenue')
plt.gca().invert_yaxis()  # Invert y-axis to have the highest revenue at the top
plt.show()

# Identify least-selling products by total revenue
least_selling_products_df = product_sales_df.orderBy(col("Total_Revenue"))

# Convert the least 20 products by revenue to Pandas DataFrame
least_selling_products_pd = least_selling_products_df.limit(20).toPandas()

# Plot
plt.figure(figsize=(10, 6))
plt.barh(least_selling_products_pd['Product_Name'], least_selling_products_pd['Total_Revenue'], color='skyblue')
plt.xlabel('Total Revenue')
plt.ylabel('Product')
plt.title('least 20 Products by Total Revenue')
plt.gca().invert_yaxis()  # Invert y-axis to have the highest revenue at the top
plt.show()

# Identify top-selling products by sales volume
top_selling_products_by_volume_df = product_sales_df.orderBy(col("Sales_Volume").desc())

# Convert the top 20 products by revenue to Pandas DataFrame
top_selling_products_by_volume_pd = top_selling_products_by_volume_df.limit(20).toPandas()

# Plot
plt.figure(figsize=(10, 6))
plt.barh(top_selling_products_by_volume_pd['Product_Name'], top_selling_products_by_volume_pd['Sales_Volume'], color='lightgreen')
plt.xlabel('Sales Volume')
plt.ylabel('Product')
plt.title('Top 10 Products by Sales Volume')
plt.gca().invert_yaxis()  # Invert y-axis to have the highest revenue at the top
plt.show()

# Identify least-selling products by sales volume
least_selling_products_by_volume_df = product_sales_df.orderBy(col("Sales_Volume"))

# Convert the top 20 products by revenue to Pandas DataFrame
least_selling_products_by_volume_pd = least_selling_products_by_volume_df.limit(20).toPandas()

# Plot
plt.figure(figsize=(10, 6))
plt.barh(least_selling_products_by_volume_pd['Product_Name'], least_selling_products_by_volume_pd['Sales_Volume'], color='lightgreen')
plt.xlabel('Sales Volume')
plt.ylabel('Product')
plt.title('least 20 Products by Sales Volume')
plt.gca().invert_yaxis()  # Invert y-axis to have the highest revenue at the top
plt.show()

from pyspark.sql.functions import corr

# Analyze the correlation between price and sales volume
price_sensitivity_df = product_sales_df.withColumn("Price", col("Price"))
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

# Calculate the percentage contribution of each product to total revenue
total_revenue = product_sales_df.agg(sum("Total_Revenue")).first()[0]

product_sales_df = product_sales_df.withColumn(
    "Revenue_Contribution_Percent",
    (col("Total_Revenue") / total_revenue) * 100
)

# Convert to Pandas DataFrame
# Top products which contribute in total revenue
product_sales_pd = product_sales_df.orderBy("Revenue_Contribution_Percent", ascending=False).limit(20).toPandas()

# Plot
plt.figure(figsize=(10, 6))
plt.barh(product_sales_pd['Product_Name'], product_sales_pd['Revenue_Contribution_Percent'], color='salmon')
plt.xlabel('Revenue Contribution (%)')
plt.ylabel('Product')
plt.title('Top 20 Products by Revenue Contribution')
plt.gca().invert_yaxis()
plt.show()

# # Calculate the percentage contribution of each category to total revenue
# total_category_revenue = category_performance_df.agg(sum("Category_Total_Revenue")).first()[0]

# category_performance_df = category_performance_df.withColumn(
#     "Category_Revenue_Contribution_Percent",
#     (col("Category_Total_Revenue") / total_category_revenue) * 100
# )

# # Convert to Pandas DataFrame
# category_contribution_pd = category_performance_df.toPandas()

# # Plot
# plt.figure(figsize=(8, 8))
# plt.pie(category_contribution_pd['Category_Revenue_Contribution_Percent'], labels=category_contribution_pd['Category'],
#         autopct='%1.1f%%', startangle=140, colors=plt.cm.Accent.colors)
# plt.title('Category Contribution to Total Revenue')
# plt.show()

# Monthly sales patterns
# Convert the Date column to a proper date type
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

