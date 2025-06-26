import pandas as pd
from pyspark.sql.functions import trim, col, to_date, current_date,initcap,regexp_replace,expr
from pyspark.sql.types import DecimalType
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Financial") \
        .getOrCreate()

def trim_string(df):
    string_columns = [field.name for field in df.schema.fields if field.dataType.simpleString() == 'string']

    for col_name in string_columns:
        df = df.withColumn(col_name, trim(col(col_name)))

    return df

def datatypes_cleaning(df):
    # Convert 'date' to valid date
    if 'date' in df.columns:
        df_parsed_date = df.withColumn('date', to_date(col('date'), "yyyy-MM-dd"))

        df = df_parsed_date.filter(col('date').isNotNull() & (col("date") <= current_date()))

    if 'amount' in df.columns:
        df = df.withColumn("amount", regexp_replace("amount", "[₹,]", "").cast(DecimalType(10, 2)))
        df = df.filter(col('amount') >= 0)

    return df

def remove_null(df):

    preferred_column = ['name', 'amount','type']

    valid_column = [col for col in preferred_column if col in df.columns]
    if valid_column:
        df = df.dropna(subset = valid_column)

    return df

def standardize_text(df):
    text_columns = ['type', 'mode', 'category', 'source','party','name','supplier']
    for col_name in text_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, initcap(trim(col(col_name))))  # e.g., "cash " → "Cash"
    return df

def validate_references(df,df_ledger,reference_col,table_type):
    if table_type in ['sales', 'purchases']:
        df_valid = df.join(df_ledger, df[reference_col] == df_ledger['name'], 'inner')
        df_invalid = df.join(df_ledger, df[reference_col] == df_ledger['name'], 'left_anti')

    elif table_type == 'bank':
        # Step 1: Get list of valid ledger names
        ledger_names = df_ledger.select("name").rdd.flatMap(lambda x: x).collect()

        conditions = [df['description'].contains(name) for name in ledger_names]

        if conditions:
            combined_condition = conditions[0]
            for cond in conditions[1:]:
                combined_condition = combined_condition | cond

            df_valid = df.filter(combined_condition)
            df_invalid = df.filter(~combined_condition)
        
        else:
            df_valid = spark.createDataFrame([], df.schema)
            df_invalid = df

    else:
        raise ValueError("Invalid table_type provided.")

    return df_valid, df_invalid

def clean_data(df):
    trim_string(df)
    standardize_text(df)
    datatypes_cleaning(df)
    remove_null(df)

    return df


    