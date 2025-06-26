import os
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

def get_partitioned_csv_file_path(folder_path):
    # Look for a part-*.csv file inside the folder
    for file_name in os.listdir(folder_path):
        if file_name.startswith("part-") and file_name.endswith(".csv"):
            return os.path.join(folder_path, file_name)
    raise FileNotFoundError("No partitioned CSV file found in folder.")

# Load each csvs
def load_csv_to_mysql(csv_path,table_name):
    df = pd.read_csv(csv_path)
    df.to_sql(name=f"{table_name}_cleaned",con=engine,if_exists='replace',index=False)
    print(f"------- Loaded {table_name} into MySQL.-------")