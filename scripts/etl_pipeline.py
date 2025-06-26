import pandas as pd
from sqlalchemy import create_engine
import os
import sys
from pyspark.sql import SparkSession

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.data_cleaning import clean_data, validate_references
from utils.loading_data import get_partitioned_csv_file_path, load_csv_to_mysql

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
processed_dir = os.path.join(project_root, 'data', 'processed')
os.makedirs(processed_dir, exist_ok=True)

spark = SparkSession.builder.getOrCreate()

# File paths for each dataset (update according to your folder structure)
files = {
    "ledgers_cleaned": "./data/ledgers.csv",
    "sales_cleaned": "./data/sales.csv",
    "purchases_cleaned": "./data/purchases.csv",
    "bank_transactions_cleaned": "./data/bank.csv"
}

# Cleaning data removing null, and trimming data
def cleaning_data(table,file_path):
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    df = clean_data(df)

    path = os.path.join(processed_dir, table)
    df.write.csv(path,header=True,mode="overwrite")

# validating foreign key reference
def validating_reference():
    for table,file in files.items():
        df_name = table.split('_')[0]

        file_path = os.path.join(processed_dir,table)
        # sales validation
        if df_name == 'sales':
            df_sales = spark.read.csv(file_path, header=True, inferSchema=True)
        elif df_name == 'ledgers':
            df_ledger = spark.read.csv(file_path, header=True, inferSchema=True)
        elif df_name == 'purchases':
            df_purchases = spark.read.csv(file_path, header=True, inferSchema=True)
        else:
            df_bank = spark.read.csv(file_path, header=True, inferSchema=True)
    
    sales_valid, sales_invalid = validate_references(df_sales, df_ledger, 'party', 'sales')
    sales_path = os.path.join(processed_dir, "valid_sales")
    sales_valid.write.csv(sales_path,header=True,mode="overwrite")

    invalid_sales_path = os.path.join(processed_dir, "invalid_sales")
    sales_invalid.write.csv(invalid_sales_path,header=True,mode="overwrite")

    purchases_valid, purchases_invalid = validate_references(df_purchases, df_ledger, 'supplier', 'purchases')
    purchases_path = os.path.join(processed_dir, "valid_purchases")
    purchases_valid.write.csv(purchases_path,header=True,mode="overwrite")

    invalid_purchases_path = os.path.join(processed_dir, "invalid_purchases")
    purchases_invalid.write.csv(invalid_purchases_path,header=True,mode="overwrite")

    bank_valid, bank_invalid = validate_references(df_bank, df_ledger, 'description', 'bank')
    bank_path = os.path.join(processed_dir, "valid_bank")
    bank_valid.write.csv(bank_path,header=True,mode="overwrite")

    invalid_bank_path = os.path.join(processed_dir, "invalid_bank")
    bank_invalid.write.csv(invalid_bank_path,header=True,mode="overwrite")

    ledger_path = os.path.join(processed_dir,"valid_ledgers")
    df_ledger.write.csv(ledger_path,header=True,mode="overwrite")

# Cleaning and transforming data
def transform_data():
    # Clean data selecting each file from list
    for table,file in files.items():
        cleaning_data(table,file)
    print("Data is cleaned---------")

    # Validate reference for foreign key existence
    validating_reference()
    print("Data is validated---------")

# Load data to sql
def load_data():
    tables = ["sales", "purchases", "ledgers", "bank"]
    for table in tables:
        folder_path = os.path.join(processed_dir, f"valid_{table}")
        csv_path = get_partitioned_csv_file_path(folder_path) 
        load_csv_to_mysql(csv_path,table)
        print(f"-----{table} loaded to database-----")

transform_data()
load_data()

