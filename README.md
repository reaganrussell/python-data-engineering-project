# Acquiring and Processing Information on the World's Largest Banks

## Project Overview
This project implements an automated ETL (Extract, Transform, Load) process to compile, transform, and store data on the top 10 largest banks in the world ranked 
by market capitalization. The processed data is stored in CSV format and a database for further analysis.
Please see [Project Description](project_description.pdf) for project guidelines per the IBM course.

> [!NOTE]  
> This README was mostly generated using AI for sake of time (finals at the time of me writing this!).
> Everything was double checked to ensure accuracy, but please take this into account.

## Key Features
1. **Web Scraping**: Extracts tabular data from Wikipedia.
2. **Data Transformation**: Converts market capitalization values from USD to GBP, EUR, and INR using exchange rates from a CSV file.
3. **Data Storage**: Saves the transformed data to:
   - A local CSV file.
   - An SQLite database table.
4. **Query Execution**: Runs SQL queries to analyze and retrieve data.
5. **Logging**: Logs progress and errors in a dedicated log file for tracking execution.

## Files in the Project
- **`banks_project.py`**: The main script containing all the code.
- **`exchange_rate.csv`**: Contains exchange rates for currency conversion.
- **`Largest_banks_data.csv`**: Output file with the processed data in CSV format.
- **`Banks.db`**: SQLite database file storing the processed data.
- **`code_log.txt`**: Log file documenting the progress and any errors encountered.

## Workflow
1. **Logging Function**: Logs execution stages in `code_log.txt`.
2. **Data Extraction**: Retrieves bank data from a web source using BeautifulSoup.
3. **Data Transformation**:
   - Adds market capitalization values in GBP, EUR, and INR based on exchange rates.
   - Rounds values to two decimal places.
4. **Data Loading**:
   - Saves the data to a CSV file.
   - Loads the data into an SQLite database table.
5. **SQL Querying**: Runs predefined SQL queries to:
   - Retrieve all table data.
   - Calculate the average market capitalization in GBP.
   - Retrieve the names of the top 5 banks.
6. **Logging Verification**: Ensures all execution stages are logged in `code_log.txt`.

## Setup and Execution
1. Install required Python libraries:
   ```bash
   pip install requests bs4 pandas numpy
2. Run the file using:
   ```bash
   python<version> banks_project.py
i.e python3.12 banks_project.py

## Developer
Reagan Russell

