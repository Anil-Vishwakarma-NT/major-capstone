import sys
import os
sys.path.append(os.path.abspath('..'))
import logging 
def verify_and_handle_verification(data_dictionary):
    try:
        logging.info("Starting data accuracy verification and cleaning")

        customer_df = data_dictionary["customers"]
        interactions_df = data_dictionary["interactions"]
        transactions_df = data_dictionary["transactions"]
        product_df = data_dictionary["products"]
        sales_team_df = data_dictionary["sales_team"]
        
        # verify that dataframe's id (primary key should not be null)
        customer_df = customer_df.filter(customer_df["Customer_ID"].isNotNull())
        interactions_df = interactions_df.filter(interactions_df["Interaction_ID"].isNotNull())
        transactions_df = transactions_df.filter(transactions_df["Transaction_ID"].isNotNull())
        product_df = product_df.filter(product_df["Product_ID"].isNotNull())
        sales_team_df = sales_team_df.filter(sales_team_df["Sales_Rep_ID"].isNotNull())

        # Verify and Clean Transaction Data: Customer_ID validation
        invalid_customer_records_in_transaction = transactions_df.join(customer_df, "customer_id", "left_anti")
        invalid_customer_record_in_transaction_count = invalid_customer_records_in_transaction.count()
        logging.info(f"Invalid customer records in transactions: {invalid_customer_record_in_transaction_count}")

        if invalid_customer_record_in_transaction_count > 0:
            # Remove invalid customer records from transactions
            transactions_df = transactions_df.join(customer_df, "customer_id", "left_semi")

        # Verify and Clean Interaction Data: Customer_ID validation
        invalid_customer_records_in_interaction = interactions_df.join(customer_df, "customer_id", "left_anti")
        invalid_customer_record_in_interaction_count = invalid_customer_records_in_interaction.count()
        logging.info(f"Invalid customer records in interactions: {invalid_customer_record_in_interaction_count}")

        if invalid_customer_record_in_interaction_count > 0:
            # Remove invalid customer records from interactions
            interactions_df = interactions_df.join(customer_df, "customer_id", "left_semi")

        # Verify and Clean Transaction Data: Product_ID validation
        invalid_products_records_in_transaction = transactions_df.join(product_df, "product_id", "left_anti")
        invalid_products_record_in_transaction_count = invalid_products_records_in_transaction.count()
        logging.info(f"Invalid product records in transactions: {invalid_products_record_in_transaction_count}")

        if invalid_products_record_in_transaction_count > 0:
            # Remove invalid product records from transactions
            transactions_df = transactions_df.join(product_df, "product_id", "left_semi")

        # Verify and Clean Transaction Data: Sales_Rep_ID validation
        invalid_sales_rep_records_in_transactions = transactions_df.join(sales_team_df, "sales_rep_id", "left_anti")
        invalid_sales_rep_in_transactions_count = invalid_sales_rep_records_in_transactions.count()
        logging.info(f"Invalid sales rep records in transactions: {invalid_sales_rep_in_transactions_count}")

        if invalid_sales_rep_in_transactions_count > 0:
            # Remove invalid sales rep records from transactions
            transactions_df = transactions_df.join(sales_team_df, "sales_rep_id","left_semi")

        # Final check: Return cleaned data or indicate verification passed
        if (invalid_customer_record_in_transaction_count == 0 and
                invalid_customer_record_in_interaction_count == 0 and
                invalid_products_record_in_transaction_count == 0 and
                invalid_sales_rep_in_transactions_count == 0):
            logging.info("Data accuracy verified: All records are valid")
            return True, data_dictionary
        else:
            logging.warning("Data accuracy verification failed: Invalid records removed")

            # Return cleaned DataFrames in the dictionary
            cleaned_data_dictionary = {
                "customers": customer_df,
                "interactions": interactions_df,
                "transactions": transactions_df,
                "products": product_df,
                "sales_team": sales_team_df
            }

            return False, cleaned_data_dictionary

    except Exception as e:
        logging.error(f"Error verifying and cleaning data: {e}", exc_info=True)
        sys.exit("Exiting due to an unexpected error")
