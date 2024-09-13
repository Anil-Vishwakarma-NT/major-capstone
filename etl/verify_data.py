import sys
import os
sys.path.append(os.path.abspath('..'))
import logging

# Verifying data accuracy between two dataFrames
def verify_data_accuracy(data_dictionary):
    try:
        logging.info("Starting data accuracy verification")

        customer_df = data_dictionary["customers"]
        interactions_df = data_dictionary["interactions"]
        transactions_df = data_dictionary["transactions"]
        product_df = data_dictionary["products"]
        sales_team_df = data_dictionary["sales_team"]

        # Confirm that Customer_ID in transactions.csv and customers.csv matches correctly.
        invalid_customer_record_in_transaction_count = transactions_df.join(customer_df, "customer_id", "left_anti").count()
        logging.info(f"Invalid customer records in transactions: {invalid_customer_record_in_transaction_count}")

        # Confirm that Customer_ID in interactions.csv and customers.csv matches correctly.
        invalid_customer_record_in_interaction_count = interactions_df.join(customer_df, "customer_id", "left_anti").count()
        logging.info(f"Invalid customer records in interactions: {invalid_customer_record_in_interaction_count}")

        # Check that Product_ID in transactions.csv is valid according to products.csv
        invalid_products_record_in_transaction_count = transactions_df.join(product_df, "product_id", "left_anti").count()
        logging.info(f"Invalid product records in transactions: {invalid_products_record_in_transaction_count}")

        # Ensure that Sales_Rep_ID in transactions.csv matches entries in sales_team.csv.
        invalid_sales_rep_in_transactions_count = transactions_df.join(sales_team_df, "sales_rep_id", "left_anti").count()
        logging.info(f"Invalid sales rep records in transactions: {invalid_sales_rep_in_transactions_count}")

        if (invalid_customer_record_in_transaction_count == 0 and
                invalid_customer_record_in_interaction_count == 0 and
                invalid_products_record_in_transaction_count == 0 and
                invalid_sales_rep_in_transactions_count == 0):
            logging.info("Data accuracy verified: All records are valid")
            return True
        else:
            logging.warning("Data accuracy verification failed: There are invalid records")
            return False

    except Exception as e:
        logging.error(f"Error verifying data accuracy: {e}", exc_info=True)
        return False