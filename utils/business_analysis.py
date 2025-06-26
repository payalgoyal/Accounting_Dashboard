from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, col,lit, sum,when, udf, trim, lower, to_date
from pyspark.sql.types import StructType, StructField, StringType
import matplotlib.pyplot as plt

import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
analysed_dir = os.path.join(project_root, 'data', 'analysed')
os.makedirs(analysed_dir, exist_ok=True)

jar_path = os.path.join(project_root, "mysql-connector-j-9.3.0", "mysql-connector-j-9.3.0.jar")
spark = SparkSession.builder \
    .appName("Financial Analysis") \
    .config("spark.jars", jar_path)\
    .getOrCreate()

# DB config
db_config = {
    "host": "localhost",
    "port": 3306,
    "user" : 'root',
    "password" : "YourPasswordHere",  # replace with your actual password
    "db" : 'financial_analysis',
    "url": "jdbc:mysql://localhost:3306/financial_analysis"
}

@udf(returnType=StructType([
    StructField("party", StringType()),
    StructField("type", StringType())
]))

#split description v=column in bank df to get party name and type
def split_description(desc):
    left, _, right = desc.rpartition(" ")
    return (left.strip(), right.strip())

def analysis():

    #Read data from sql in df
    sales_df, purchases_df, bank_df, ledgers_df = read_from_sql()

    # monthly trend of sales and purchases
    monthly_trend_analysis(sales_df, purchases_df)

    # Party wise outstanding
    party_wise_outstanding(sales_df,purchases_df,bank_df,ledgers_df)

    #Cash flow analysis
    cash_flow(bank_df)

def read_from_sql():
    # Sales table
    sales_df = spark.read \
    .format("jdbc") \
    .option("url", db_config["url"]) \
    .option("dbtable", "sales_cleaned") \
    .option("user", db_config["user"]) \
    .option("password", db_config["password"]) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

    # Purchases table
    purchases_df = spark.read \
    .format("jdbc") \
    .option("url", db_config["url"]) \
    .option("dbtable", "purchases_cleaned") \
    .option("user", db_config["user"]) \
    .option("password", db_config["password"]) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

    # Bank table
    bank_df = spark.read \
    .format("jdbc") \
    .option("url", db_config["url"]) \
    .option("dbtable", "bank_cleaned") \
    .option("user", db_config["user"]) \
    .option("password", db_config["password"]) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

    # Ledgers table
    ledgers_df = spark.read \
    .format("jdbc") \
    .option("url", db_config["url"]) \
    .option("dbtable", "ledgers_cleaned") \
    .option("user", db_config["user"]) \
    .option("password", db_config["password"]) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

    return sales_df,purchases_df,bank_df,ledgers_df

def monthly_trend_analysis(sales_df, purchases_df):
    # Extract month and aggregate sales
    sales_monthly = sales_df.withColumn("month", date_format(col("date"), "yyyy-MM")) \
                            .groupBy("month") \
                            .sum("amount") \
                            .withColumnRenamed("sum(amount)", "total_sales")

    # Extract month and aggregate purchases
    purchases_monthly = purchases_df.withColumn("month", date_format(col("date"), "yyyy-MM")) \
                                    .groupBy("month") \
                                    .sum("amount") \
                                    .withColumnRenamed("sum(amount)", "total_purchases")

    monthly_trend = sales_monthly.join(purchases_monthly, on="month", how="outer").orderBy("month")
    monthly_trend = monthly_trend.fillna(0)

    path = os.path.join(analysed_dir,"monthly_trend.csv")
    monthly_trend.toPandas().to_csv(path, index=False)

def party_wise_outstanding(sales_df,purchases_df,bank_df,ledgers_df):
    sales_df = sales_df.withColumn("amount", col("amount").cast("double"))
    purchases_df = purchases_df.withColumn("amount", col("amount").cast("double"))
    bank_df = bank_df.withColumn("amount", col("amount").cast("double"))

    # Rename for joins
    ledgers_df = ledgers_df.withColumnRenamed("name", "party_name") \
                       .withColumnRenamed("type", "party_type")
    
    # Add type to sales (customers)
    sales_df = sales_df.withColumnRenamed("party", "party_name") 
    sales_df = sales_df.join(ledgers_df, on="party_name", how="left")

     # Add type to purchases (suppliers)
    purchases_df = purchases_df.withColumnRenamed("supplier", "party_name")
    purchases_df = purchases_df.join(ledgers_df, on="party_name", how="left")

    sales_df = sales_df.withColumn("nature", lit("debit"))
    purchases_df = purchases_df.withColumn("nature", lit("credit"))

    # Bank party name (assuming description field has party name)
    bank_df = bank_df.withColumn("split", split_description("description"))
    bank_df = bank_df.withColumn("party_name", col("split").getItem("party")) \
                 .withColumn("txn_type", col("split").getItem("type"))
    
    bank_df = bank_df.withColumn("nature",when(col("txn_type") == "payment","credit").otherwise("debit"))

    bank_df = bank_df.join(
        ledgers_df.select(col("party_name"), col("party_type")),
        on="party_name",
        how="left"
    )

    combined_df = sales_df.select("party_name", "amount", "nature", "party_type") \
    .union(purchases_df.select("party_name", "amount", "nature", "party_type")) \
    .union(bank_df.select("party_name", "amount", "nature","party_type"))

    # Group by party_name and type (customer or supplier)
    outstanding_df = combined_df.groupBy("party_name", "party_type").agg(
        sum(when(col("nature") == "debit", col("amount")).otherwise(0)).alias("total_debit"),
        sum(when(col("nature") == "credit", col("amount")).otherwise(0)).alias("total_credit")
    )

    # Calculate outstanding balance (debit - credit)
    result_df = outstanding_df.withColumn("outstanding", col("total_debit") - col("total_credit"))

    result_df.orderBy("party_name").show(truncate=False)

    path = os.path.join(analysed_dir,"party_outstanding.csv")
    result_df.toPandas().to_csv(path, index=False)

def cash_flow(bank_df):
    bank_df = bank_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    bank_df = bank_df.withColumn("month", date_format(col("date"), "yyyy-MM"))

    cash_flow_df = bank_df.groupBy("month").agg(
        sum(when(col("type") == "credit", col("amount")).otherwise(0)).alias("total_inflow"),
        sum(when(col("type") == "debit", col("amount")).otherwise(0)).alias("total_outflow")
    )

    cash_flow_df = cash_flow_df.withColumn("net_cash_flow", col("total_inflow") - col("total_outflow"))

    # Convert to Pandas
    pdf = cash_flow_df.toPandas()

    # Plot
    ax = pdf.plot(
        x="month",
        y=["total_inflow", "total_outflow", "net_cash_flow"],
        kind="bar",
        figsize=(10, 6),
        title="Monthly Cash Flow"
    )

    # Labeling (optional but recommended)
    plt.xlabel("Month")
    plt.ylabel("Amount")
    plt.tight_layout()

    # Save as PNG
    plt.savefig(os.path.join(project_root, 'data', 'analysed',"monthly_cash_flow.png") )

analysis()