import os
import sys
sys.path.append(os.path.abspath('..'))
import matplotlib.pyplot as plt
import seaborn as sns
import logging
from pyspark.sql.functions import col, when, count, round, year, month, first
from utils.read_write import read_csv_files
from utils.visualization import bar_graph,stacked_plot, plot_pie_chart
from utils.logger_setup import setup_logging
# import the path variable
from utils.get_config_data import CLEANED_DATA_DIR

logging.info("CUSTOMER INTERACTION EFFECTIVENESS ANALYSIS.................")
# Customer Interaction effectiveness Analysis
try:
    logging.info("reading csv files")    
    # read customers data
    customers_df = read_csv_files(f"{CLEANED_DATA_DIR}/customers.csv")
    # read transactions data
    interactions_df = read_csv_files(f"{CLEANED_DATA_DIR}/interactions.csv")
    interactions_df.show()
except Exception as e:
    logging.error("Error in loading files :{e}", exc_info=True)
    # stop the execution
    sys.exit("Exiting due to an unexpected error")
###  Resolution Rates For Each Interaction Type

try: 
    logging.info("Resolution Rate For Each Interaction Type")   
    # Calculate resolution rates for each interaction type
    resolution_rates_df = interactions_df.groupBy("Interaction_Type").agg(
        count("Issue_Resolved").alias("Total_Interactions"),
        count(when(col("Issue_Resolved") == "Yes", True)).alias("Resolved_Issues")) \
        .withColumn("Resolution_Rate", round(col("Resolved_Issues") / col("Total_Interactions") * 100, 2))

    resolution_rates_df = resolution_rates_df.orderBy(col("Resolution_Rate").desc())
    # show data
    resolution_rates_df.show()
    # Extract lists for plotting
    categories = [row['Interaction_Type'] for row in resolution_rates_df.collect()]
    revenues = [row['Resolution_Rate'] for row in resolution_rates_df.collect()]
    # Define the explode values (can be customized)
    explode = [0.05 if i == 0 else 0 for i in range(len(categories))]
    # Create a figure and axes object
    fig, ax = plt.subplots(figsize=(6, 6))
    # Call the generalized pie chart function
    plot_pie_chart(
        ax,
        sizes=revenues,
        labels=categories,
        explode=explode,
        title="Resolution Rate Contribution Of Interaction Types")
    # show pie chart
    plt.show()
    
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)
     
try:    
    # Join customers with interactions to analyze preferences
    customer_interaction_df = interactions_df.join(customers_df, "Customer_ID")
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True) 
   
### Calculate Preference For Each Interaction Type By Country
try:
    logging.info("Top Interaction Type By Country")
    # Calculate preference for each interaction type by country
    country_preference_interaction_type_df = customer_interaction_df.groupBy("Country", "Interaction_Type").agg(
        count("*").alias("Interaction_Count")
    ).orderBy(col("Interaction_Count").desc()).limit(10)
    # show
    country_preference_interaction_type_df.show()
    # Convert Spark DataFrame to Pandas DataFrame for plotting
    top_10_pd = country_preference_interaction_type_df.toPandas()
    # Set up plot size and parameters
    fig, ax = plt.subplots(figsize=(10, 8))
    # Plotting the data
    stacked_plot(ax,
                data=top_10_pd, 
                x_col="Country", 
                y_col="Interaction_Count", 
                hue_col="Interaction_Type", 
                title="Top Interaction Types By Country",
                x_label="Country", 
                y_label="Total Interaction Count"
    )
    # show stacked graph
    plt.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)     
    
### Calculate Countries With Highest Interaction Count

try:
    logging.info("Top Countries With Highest Interaction Count")    
    # Calculate countries with interaction count
    country_preference_df = customer_interaction_df.groupBy("Country") \
        .agg(count("*").alias("Total_Interaction_Count")) \
        .orderBy(col("Total_Interaction_Count").desc())
    # select country and interaction count
    top_10_country_preference_df = country_preference_df.select("Country", "Total_Interaction_Count").limit(10)
    # show dataframe
    top_10_country_preference_df.show()   
    # Convert the columns from Spark DataFrame to Python lists
    categories = [row['Country'] for row in top_10_country_preference_df.collect()]
    values = [row['Total_Interaction_Count'] for row in top_10_country_preference_df.collect()] 
    # Create figure and axis for the bar graph
    fig, ax = plt.subplots(figsize=(10, 6)) 
    # Call the bar_graph function
    bar_graph(ax,
            categories=categories,
            values=values,
            title="Top Countries By Interaction Count",
            xlabel="Country",
            ylabel="Total Interaction Count",
            color='#B2B0EA',  
            edgecolor='#3C3D99')   
    # Show bar chart
    plt.show()

except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True) 

### Calculate The Issue With Respect To Particular Customer And Interaction Type
try:
    logging.info("Top Customers With Interaction Type Who Raised Most Interaction")
    #calculate the issue with respect to particular customer and interaction type
    customer_interaction_preference = customer_interaction_df.groupBy("Interaction_Type","Customer_id") \
        .agg(first("Name").alias("Name"),
            count("*").alias("issue_raised")) \
        .orderBy("issue_raised",ascending = False)
    # Select Name, Interaction Type, issue raised
    customer_interaction_preference=customer_interaction_preference.select("Name","Interaction_Type","issue_raised") \
        .orderBy(col("issue_raised").desc()) \
        .limit(13)
    # show dataframe    
    customer_interaction_preference.show()
    # Set up plot size and parameters
    fig, ax = plt.subplots(figsize=(10, 6))
    # Plotting the data
    stacked_plot(ax,
        data=customer_interaction_preference.toPandas(), 
        x_col="Name", 
        y_col="issue_raised", 
        hue_col="Interaction_Type",
        title="Top Customers With High Interaction Type",
        x_label="Customer Name",
        y_label="Number of Issues Raised"
    )

except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)     
try:
    logging.info("Top Customers With Maximum Interaction Count")    
    # Calculate countries with interaction count
    country_customer_df = customer_interaction_df.groupBy("Customer_id") \
        .agg(count("*").alias("Total_Interaction_Count"),
         first("Name").alias("Name")) \
        .orderBy(col("Total_Interaction_Count").desc())
    # select country and interaction count
    top_10_customer_interaction_df = country_customer_df.select("Name", "Total_Interaction_Count").limit(10)
    # show dataframe
    top_10_customer_interaction_df.show()   
    # Convert the columns from Spark DataFrame to Python lists
    categories = [row['Name'] for row in top_10_customer_interaction_df.collect()]
    values = [row['Total_Interaction_Count'] for row in top_10_customer_interaction_df.collect()] 
    # Create figure and axis for the bar graph
    fig, ax = plt.subplots(figsize=(10, 6)) 
    # Call the bar_graph function
    bar_graph(ax,
            categories=categories,
            values=values,
            title="Top Customers By Interaction Count",
            xlabel="Customers",
            ylabel="Total Interaction Count",
            color='#B2B0EA',  
            edgecolor='#3C3D99')   
    # Show bar chart
    plt.show()

except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True) 
### Calculate Resolution Rates By Year And Month
try:
    logging.info("Calculate Resolution Rates By Year and Month")    
    # Extract year and month from interaction date
    interactions_df = interactions_df.withColumn("Year", year("Interaction_Date"))
    interactions_df = interactions_df.withColumn("Month", month("Interaction_Date"))
    # Calculate resolution rates by year and month
    monthly_resolution_df = interactions_df.groupBy("Year", "Month", "Interaction_Type").agg(
        count("*").alias("Total_Interactions"),
        count(when(col("Issue_Resolved") == "Yes", True)).alias("Resolved_Issues")
    ).withColumn(
        "Resolution_Rate", round(col("Resolved_Issues") / col("Total_Interactions") * 100, 2)
    ).orderBy("Year", "Month")
    # show dataframe
    monthly_resolution_df.show()
        
    # Convert to Pandas DataFrame for plotting
    monthly_resolution_pd = monthly_resolution_df.toPandas()

    # Plot using Matplotlib
    plt.figure(figsize=(12, 8))
    # Use 'Interaction_Type' as part of the color palette definition
    sns.lineplot( 
        data=monthly_resolution_pd, 
        x='Month', 
        y='Resolution_Rate', 
        hue='Interaction_Type',  
        style='Interaction_Type',  
        markers=True,
        palette='Set1'   
    )
    # Add labels and title
    plt.xlabel('Months(2024)', fontweight='bold',fontsize = 15)
    plt.ylabel('Resolution Rate (%)', fontweight='bold',fontsize = 15)
    plt.title('Monthly Resolution Rates By Interaction Type', fontweight='bold',fontsize = 20)
    # Show the plot
    plt.show()     
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)     