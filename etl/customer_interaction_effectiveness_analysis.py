
import os
import sys
sys.path.append(os.path.abspath('..'))
import matplotlib.pyplot as plt
import seaborn as sns
import logging

from pyspark.sql.functions import col, when, count, round, year, month
from utils.read import read_csv_files
from utils.visualization import bar_graph,stacked_plot, plot_pie_chart

# Commented out IPython magic to ensure Python compatibility.
# logger setup
# %cd /spark-data/capstone_crm/utils
# %run 'logger_setup.ipynb'

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
    # stop the execution
    sys.exit("Exiting due to an unexpected error")

"""###  resolution rates for each interaction type"""

try:
    logging.info("Resolution Rate For each Interaction Type")
    # Calculate resolution rates for each interaction type
    resolution_rates_df = interactions_df.groupBy("Interaction_Type").agg(
        count("Issue_Resolved").alias("Total_Interactions"),
        count(when(col("Issue_Resolved") == "Yes", True)).alias("Resolved_Issues")
    ).withColumn(
        "Resolution_Rate", round(col("Resolved_Issues") / col("Total_Interactions") * 100, 2))

    resolution_rates_df = resolution_rates_df.orderBy(col("Resolution_Rate").desc())
    resolution_rates_df.show()
    # Extract lists for plotting
    categories = [row['Interaction_Type'] for row in resolution_rates_df.collect()]
    revenues = [row['Resolution_Rate'] for row in resolution_rates_df.collect()]
    # Define the explode values (optional, can be customized)
    explode = [0.05 if i == 0 else 0 for i in range(len(categories))]  # Emphasizing the highest revenue category
    # Create a figure and axes object
    fig, ax = plt.subplots(figsize=(6, 6))
    # Call the generalized pie chart function
    plot_pie_chart(ax,sizes=revenues,labels=categories,explode=explode,
            title="Resolution Rate Contribution For Each Interaction Type")
    # show pie chart
    plt.show()

except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)
    raise

try:
    # Join customers with interactions to analyze preferences
    customer_interaction_df = interactions_df.join(customers_df, "Customer_ID")
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)
    raise

"""### Calculate preference for each interaction type by country"""

try:
    logging.info("Top 10 Interaction type by country")
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
    stacked_plot(ax, data=top_10_pd, x_col="Country", y_col="Interaction_Count",
        hue_col="Interaction_Type", title="Top 10 Interaction Types by Country",
        x_label="Country", y_label="Total Interaction Count"
    )
    # show stacked graph
    plt.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)
    raise

"""### Calculate countries with highest Interaction count"""

try:
    logging.info("top 10 countries with highest interaction count")
    # Calculate countries with interaction count
    country_preference_df = customer_interaction_df.groupBy("Country").agg(count("*").alias("Total_Interaction_Count")) \
    .orderBy(col("Total_Interaction_Count").desc())
    # select country and interaction count
    top_10_country_preference_df = country_preference_df.select("Country", "Total_Interaction_Count").limit(10)
    # Convert the columns from Spark DataFrame to Python lists
    categories = [row['Country'] for row in top_10_country_preference_df.collect()]
    values = [row['Total_Interaction_Count'] for row in top_10_country_preference_df.collect()]
    # Create figure and axis for the bar graph
    fig, ax = plt.subplots(figsize=(10, 6))  # Customize the size if needed
    # Call the bar_graph function
    bar_graph(ax,categories=categories,values=values,title="Top 10 Countries by Interaction Count",xlabel="Country",
        ylabel="Total Interaction Count",color='#B2B0EA',  edgecolor='#3C3D99')
    # Show bar chart
    plt.show()

except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)
    raise

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
    # Select Name, Interaction Type, issue raised
    customer_interaction_preference=customer_interaction_preference.select("Name","Interaction_Type","issue_raised").orderBy(col("issue_raised").desc()).limit(13)
    customer_interaction_preference.show()
    # Set up plot size and parameters
    fig, ax = plt.subplots(figsize=(10, 6))
    # Plotting the data
    stacked_plot(ax,
        data=customer_interaction_preference.toPandas(), x_col="Name",
        y_col="issue_raised", hue_col="Interaction_Type",
        title="Top 10 Customers by Interaction Type",x_label="Customer Name",
        y_label="Number of Issues Raised"
    )

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
    # show data
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
        hue='Interaction_Type',  # Different color for each interaction type
        style='Interaction_Type',  # Different style for each interaction type
        markers=True,
        palette='Set1'
    )
    # Add labels and title
    plt.xlabel('Months(2024)', fontweight='bold',fontsize = 15)
    plt.ylabel('Resolution Rate (%)', fontweight='bold',fontsize = 15)
    plt.title('Monthly Resolution Rates by Interaction Type', fontweight='bold',fontsize = 20)
    # Show the plot
    plt.show()
except Exception as e:
    logging.error("invalid operation performed :{e}", exc_info=True)