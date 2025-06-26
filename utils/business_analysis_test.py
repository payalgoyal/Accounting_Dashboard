from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, col,lit, sum,when, udf
from pyspark.sql.types import StructType, StructField, StringType

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
    "password" : "MySqlDb@1",
    "db" : 'financial_analysis',
    "url": "jdbc:mysql://localhost:3306/financial_analysis"
}

@udf(returnType=StructType([
    StructField("party", StringType()),
    StructField("type", StringType())
]))

def split_description(desc):
    left, _, right = desc.rpartition(" ")
    return (left.strip(), right.strip())

def analysis():

    sales_df, purchases_df, bank_df, ledgers_df = read_from_sql()
    monthly_trend_analysis(sales_df, purchases_df)

    # Party wise outstanding
    party_wise_outstanding(sales_df,purchases_df,bank_df,ledgers_df)

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

    path = os.path.join(analysed_dir,"monthly_trend")
    monthly_trend.write.csv(path,header=True,mode="overwrite")

def party_wise_outstanding(sales_df,purchases_df,bank_df,ledgers_df):
    sales_df = sales_df.withColumn("amount", col("amount").cast("double"))
    purchases_df = purchases_df.withColumn("amount", col("amount").cast("double"))
    bank_df = bank_df.withColumn("amount", col("amount").cast("double"))

    # Rename for joins
    ledgers_df = ledgers_df.select(col("name").alias("party_name"))

    # Add type to sales (customers)
    sales_df = sales_df.withColumnRenamed("party", "party_name")
    sales_df = sales_df.join(ledgers_df, on="party_name", how="left")

     # Add type to purchases (suppliers)
    purchases_df = purchases_df.withColumnRenamed("supplier", "party_name")
    purchases_df = purchases_df.join(ledgers_df, on="party_name", how="left")

    sales_df = sales_df.withColumn("nature", lit("debit"))
    purchases_df = purchases_df.withColumn("nature", lit("credit"))

    bank_df = bank_df.withColumn(
        "nature", 
        when(col("type") == "credit", "credit")
        .otherwise("debit")
    )

    # Bank party name (assuming description field has party name)
    bank_df = bank_df.withColumn("split", split_description("description"))
    bank_df = bank_df.withColumn("party_name", col("split").getItem("party")) \
                 .withColumn("type", col("split").getItem("type"))

    bank_df = bank_df.withColumn(
        "nature", 
        when(col("type") == "credit", "credit")
        .otherwise("debit")
    )

    bank_df.show()

    combined_df = sales_df.select("party_name", "amount", "nature", "type") \
    .union(purchases_df.select("party_name", "amount", "nature", "type"))

    # Group by party_name and type (customer or supplier)
    outstanding_df = combined_df.groupBy("party_name", "type").agg(
        sum(when(col("nature") == "debit", col("amount")).otherwise(0)).alias("total_debit"),
        sum(when(col("nature") == "credit", col("amount")).otherwise(0)).alias("total_credit")
    )

    # Calculate outstanding balance (debit - credit)
    result_df = outstanding_df.withColumn("outstanding", col("total_debit") - col("total_credit"))

    result_df.orderBy("party_name").show(truncate=False)


analysis()