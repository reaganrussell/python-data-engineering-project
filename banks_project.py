"""
IBM Python Project for Data Engineering
Author: Reagan Russell
Project Name: Acquiring and Processing Information on the World's Largest Banks

The following tasks will be completed in this project:
Task 1: Logging Function
Task 2: Data Extraction
Task 3: Data Transformation
Task 4: Data Loading (CSV)
Task 5: Data Loading (DB)
Task 6: DB Querying
Task 7: Logging Verification

In this project, I will be going through a simple project to perform the operations of
ETL as described in the project scenario below.

Overall Project Scenario:
    You have been hired as a data engineer by research organization.
    Your boss has asked you to create a code that can be used to compile
    the list of the top 10 largest banks in the world ranked by market
    capitalization in billion USD. Further, the data needs to be transformed
    and stored in GBP, EUR and INR as well, in accordance with the exchange
    rate information that has been made available to you as a CSV file.
    The processed information table is to be saved locally in a CSV format and as a database table.

    Your job is to create an automated system to generate this information so that the same can be executed in every financial quarter to prepare the report.

"""
# Imports
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import numpy as np
import sqlite3
import requests
import os

# settings
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

# Initializing Known Entities
data_url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
csv_url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
csv_path = "./exchange_rate.csv"
table_attributes = ["Name", "MC_USD_Billion"]
output_csv_path = "./Largest_banks_data.csv"
db = "Banks.db"
table = "Largest_banks"
log_file = "code_log.txt"

def download_exchange_rate_csv(url, local_filename):
    """
    Downloads the exchange rate CSV file from the given URL and saves it locally.
    """
    try:
        log_progress("Downloading exchange rate CSV file started")
        response = requests.get(url)
        response.raise_for_status()
        with open(local_filename, 'wb') as f:
            f.write(response.content)
        log_progress("Downloading exchange rate CSV file completed")
    except Exception as e:
        log_progress(f"Downloading exchange rate CSV file failed: {e}")
        raise

# Task 1: Logging Function
def log_progress(message):
    """
    Logs the progress of the code at different stages in code_log.txt

    :param message:
    :return: None
    """
    timestamp_format = '%Y-%b-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ' : ' + message + '\n')

# Task 2: Data Extraction
def extract(data_url, table_attributes):
    """
    Extracts the tabular information from the given URL under the heading
    "By market capitalization" and saves it to a dataframe.

    :param data_url:
    :param table_attributes:
    :return: df
    """
    log_progress("Data extraction started")
    try:
        # Fetch the webpage content
        page = requests.get(data_url).text
        data = BeautifulSoup(page, 'html.parser')

        # Initialize the dataframe
        df = pd.DataFrame(columns=table_attributes)

        # Find all 'tbody' elements
        table_bodies = data.find_all('tbody')
        if not table_bodies:
            raise ValueError("No table bodies found on the webpage.")

        # Assume the first 'tbody' contains the required table
        target_table_body = table_bodies[0]
        table_rows = target_table_body.find_all('tr')

        # Iterate over each row in the table
        for row in table_rows:
            try:
                # Find all 'td' elements in the row
                columns = row.find_all('td')
                if len(columns) != 0:
                    # Extract the bank name
                    name_cell = columns[1]
                    name_links = name_cell.find_all('a')
                    if len(name_links) >= 2:
                        bank_name = name_links[1]['title']
                    else:
                        bank_name = name_cell.get_text(strip=True)

                    # Extract the market capitalization in USD billion
                    mc_usd_text = columns[2].get_text(strip=True)
                    # Remove any trailing non-digit characters
                    while mc_usd_text and not mc_usd_text[-1].isdigit():
                        mc_usd_text = mc_usd_text[:-1]
                    mc_usd = float(mc_usd_text.replace(',', '').replace('$', ''))

                    # Create a data dictionary
                    data_dict = {"Name": bank_name, "MC_USD_Billion": mc_usd}

                    # Create a temporary dataframe and concatenate it
                    df_temp = pd.DataFrame(data_dict, index=[0])
                    df = pd.concat([df, df_temp], ignore_index=True)
            except Exception as e:
                # Log any exceptions and continue
                log_progress(f"Row parsing error: {e}")
                continue

        log_progress("Data extraction completed")
        return df

    except Exception as e:
        log_progress(f"Data extraction failed: {e}")
        raise

# Task 3: Data Transformation
def transform(df):
    """
    Transform adds columns for market capitalization in GBP, EUR, and INR
    rounded to 2 decimal places based on the exchange rate information.

    :param df:
    :return: df
    """
    log_progress("Data transformation started")
    try:
        # Read exchange rates from local CSV file
        exchange_rate_df = pd.read_csv(csv_path)
        # Convert to a dictionary with "Currency" as keys and "Rate" as values
        exchange_rates = exchange_rate_df.set_index("Currency").to_dict()["Rate"]

        # Ensure 'MC_USD_Billion' is of type float
        df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(float)

        # Adds MC_GBP_Billion, MC_EUR_Billion, and MC_INR_Billion columns and round to 2 decimals
        df["MC_GBP_Billion"] = [np.round(x * exchange_rates["GBP"], 2) for x in df["MC_USD_Billion"]]
        df["MC_EUR_Billion"] = [np.round(x * exchange_rates["EUR"], 2) for x in df["MC_USD_Billion"]]
        df["MC_INR_Billion"] = [np.round(x * exchange_rates["INR"], 2) for x in df["MC_USD_Billion"]]

        log_progress("Data transformation completed")
        return df
    except Exception as e:
        log_progress(f"Data transformation failed: {e}")
        raise


# Task 4: Data Loading (CSV)
def load_to_csv(df, csv_path):
    """
    Load_to_csv saves the final dataframe as a csv file in the
    input path.

    :param df:
    :param csv_path:
    :return: None
    """
    log_progress("Data loading to CSV started")
    try:
        df.to_csv(csv_path, index=False)
        log_progress("Data loading to CSV completed")
    except Exception as e:
        log_progress(f"Data loading to CSV failed: {e}")
        raise

# Task 5: Data Loading (DB)
def load_to_db(df, sql_connection, table):
    """
    Load_to_db saves the final dataframe as a database table
    with the input name.

    :param df:
    :param sql_connection:
    :param table:
    :return: None
    """
    log_progress("Data loading to database started")
    try:
        df.to_sql(table, sql_connection, if_exists='replace', index=False)
        log_progress("Data loading to database completed")
    except Exception as e:
        log_progress(f"Data loading to database failed: {e}")
        raise

# Task 6: DB Querying
def run_query(query, sql_connection):
    """
    Run_query runs the input query on the database table, then
    prints the output in the terminal.

    :param query:
    :param sql_connection:
    :return:
    """
    log_progress("Query execution started")
    try:
        print(query)
        query_output = pd.read_sql(query, sql_connection)
        print(query_output)
        log_progress("Query execution completed")
    except Exception as e:
        log_progress(f"Query execution failed: {e}")
        raise

# Main execution
if __name__ == "__main__":
    # Clear previous log file contents
    open(log_file, 'w').close()

    # Download the exchange_rate.csv file
    download_exchange_rate_csv(csv_url, csv_path)

    # Task 2: Data Extraction
    df = extract(data_url, table_attributes)
    print("Extracted Data:")
    print(df)

    # Task 3: Data Transformation
    df = transform(df)
    print("Transformed Data:")
    print(df)

    # Task 4: Data Loading (CSV)
    load_to_csv(df, output_csv_path)

    # Task 5: Data Loading (DB)
    conn = sqlite3.connect(db)
    load_to_db(df, conn, table)

    # Task 6: DB Querying
    query = f"SELECT * FROM {table}"
    run_query(query, conn)

    # Query project/quiz
    query = f"SELECT AVG(MC_GBP_Billion) FROM {table}"
    run_query(query, conn)

    query = f"SELECT Name from {table} LIMIT 5"
    run_query(query, conn)

    # Additional queries (my own testing)
    query = f"SELECT Name, MC_USD_Billion FROM {table} ORDER BY MC_USD_Billion DESC LIMIT 1"
    run_query(query, conn)

    # Close the database connection
    conn.close()

    # Task 7: Logging Verification
    print("Log file contents:")
    with open(log_file, 'r') as f:
        print(f.read())
