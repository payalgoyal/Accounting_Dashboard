import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# DB config
user = 'root'
password = quote_plus("MySqlDb@1") 
host = 'localhost'
db = 'financial_analysis'

# Create connection
engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{db}")

# File paths
files = {
    "ledgers": "./data/ledgers.csv",
    "sales": "./data/sales.csv",
    "purchases": "./data/purchases.csv",
    "bank_transactions": "./data/bank.csv"
}

def load_raw_data(table_name, file_name):
    df = pd.read_csv(file_name)
    df.to_sql(table_name,con=engine,if_exists="append",index=False)

for table, file in files.items():
    load_raw_data(table,file)