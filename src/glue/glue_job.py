# from awsglue.context import GlueContext
# from pyspark.context import SparkContext
# from pyspark.sql.functions import col, avg
# from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# # Initialize Spark and Glue contexts
# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session

# # Define Kafka parameters
# kafka_bootstrap_servers = "broker2:29092"
# kafka_topic = "r2.public.reliance_data2"

# # Define the schema of the incoming Kafka messages
# schema = StructType([
#     StructField("payload", StructType([
#         StructField("after", StructType([
#             StructField("date", StringType(), True),
#             StructField("sales", LongType(), True),
#             StructField("expenses", LongType(), True),
#             StructField("operating_profit", LongType(), True),
#             StructField("opm_percent", LongType(), True),
#             StructField("other_income", LongType(), True),
#             StructField("interest", LongType(), True),
#             StructField("depreciation", LongType(), True),
#             StructField("profit_before_tax", LongType(), True),
#             StructField("tax_percent", DoubleType(), True),
#             StructField("net_profit", LongType(), True),
#             StructField("eps_in_rs", DoubleType(), True),
#             StructField("dividend_payout_percent", DoubleType(), True)
#         ]))
#     ]))
# ])

# # Read data from Kafka topic
# df = spark \
#     .read \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
#     .option("subscribe", kafka_topic) \
#     .option("startingOffsets", "earliest") \
#     .load() \
#     .selectExpr("CAST(value AS STRING)") \
#     .selectExpr(f"from_json(value, '{schema.simpleString()}') as data") \
#     .select("data.payload.after.*")

# # Show the schema and some data for debugging
# df.printSchema()
# df.show()

# # Calculate averages for each column
# average_values = df.agg(
#     avg("sales").alias("avg_sales"),
#     avg("expenses").alias("avg_expenses"),
#     avg("operating_profit").alias("avg_operating_profit"),
#     avg("opm_percent").alias("avg_opm_percent"),
#     avg("other_income").alias("avg_other_income"),
#     avg("interest").alias("avg_interest"),
#     avg("depreciation").alias("avg_depreciation"),
#     avg("profit_before_tax").alias("avg_profit_before_tax"),
#     avg("tax_percent").alias("avg_tax_percent"),
#     avg("net_profit").alias("avg_net_profit"),
#     avg("eps_in_rs").alias("avg_eps_in_rs"),
#     avg("dividend_payout_percent").alias("avg_dividend_payout_percent")
# )

# # Add average values as a new row to the DataFrame
# # average_row = average_values.collect()[0].asDict()
# average_row_dict = {
#     "date": "Average",
#     "sales": average_values.select("avg_sales").collect()[0][0],
#     "expenses": average_values.select("avg_expenses").collect()[0][0],
#     "operating_profit": average_values.select("avg_operating_profit").collect()[0][0],
#     "opm_percent": average_values.select("avg_opm_percent").collect()[0][0],
#     "other_income": average_values.select("avg_other_income").collect()[0][0],
#     "interest": average_values.select("avg_interest").collect()[0][0],
#     "depreciation": average_values.select("avg_depreciation").collect()[0][0],
#     "profit_before_tax": average_values.select("avg_profit_before_tax").collect()[0][0],
#     "tax_percent": average_values.select("avg_tax_percent").collect()[0][0],
#     "net_profit": average_values.select("avg_net_profit").collect()[0][0],
#     "eps_in_rs": average_values.select("avg_eps_in_rs").collect()[0][0],
#     "dividend_payout_percent": average_values.select("avg_dividend_payout_percent").collect()[0][0]
# }
# average_row_df = spark.createDataFrame([average_row_dict])
# average_row_df.show()

# # Append the averages row to the original DataFrame
# df_with_averages = df.union(average_row_df)

# # Define PostgreSQL connection properties
# postgres_url = "jdbc:postgresql://192.168.56.1:5432/concourse"
# postgres_table = "your_sink_table"
# postgres_properties = {
#     "user": "concourse_user",
#     "password": "concourse_pass",
#     "driver": "org.postgresql.Driver"
# }

# # Write data to PostgreSQL
# df_with_averages.write \
#     .format("jdbc") \
#     .option("url", postgres_url) \
#     .option("dbtable", postgres_table) \
#     .option("user", postgres_properties["user"]) \
#     .option("password", postgres_properties["password"]) \
#     .option("driver", postgres_properties["driver"]) \
#     .mode("append") \
#     .save()

# sc.stop()



from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, avg, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define Kafka parameters
kafka_bootstrap_servers = "broker2:29092"
kafka_topic = "r2.public.reliance_data2"

# Define the schema of the incoming Kafka messages
schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("date", StringType(), True),
            StructField("sales", LongType(), True),
            StructField("expenses", LongType(), True),
            StructField("operating_profit", LongType(), True),
            StructField("opm_percent", LongType(), True),
            StructField("other_income", LongType(), True),
            StructField("interest", LongType(), True),
            StructField("depreciation", LongType(), True),
            StructField("profit_before_tax", LongType(), True),
            StructField("tax_percent", DoubleType(), True),
            StructField("net_profit", LongType(), True),
            StructField("eps_in_rs", DoubleType(), True),
            StructField("dividend_payout_percent", DoubleType(), True)
        ])),
        StructField("after", StructType([
            StructField("date", StringType(), True),
            StructField("sales", LongType(), True),
            StructField("expenses", LongType(), True),
            StructField("operating_profit", LongType(), True),
            StructField("opm_percent", LongType(), True),
            StructField("other_income", LongType(), True),
            StructField("interest", LongType(), True),
            StructField("depreciation", LongType(), True),
            StructField("profit_before_tax", LongType(), True),
            StructField("tax_percent", DoubleType(), True),
            StructField("net_profit", LongType(), True),
            StructField("eps_in_rs", DoubleType(), True),
            StructField("dividend_payout_percent", DoubleType(), True)
        ])),
        StructField("source", StructType([
            StructField("ts_ms", LongType(), True)  # Extracting the timestamp field
        ]))
    ]))
])

# Read data from Kafka topic
df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .selectExpr(f"from_json(value, '{schema.simpleString()}') as data") \
    .select(
        col("data.payload.after.date").alias("date"),
        col("data.payload.after.sales").alias("sales"),
        col("data.payload.after.expenses").alias("expenses"),
        col("data.payload.after.operating_profit").alias("operating_profit"),
        col("data.payload.after.opm_percent").alias("opm_percent"),
        col("data.payload.after.other_income").alias("other_income"),
        col("data.payload.after.interest").alias("interest"),
        col("data.payload.after.depreciation").alias("depreciation"),
        col("data.payload.after.profit_before_tax").alias("profit_before_tax"),
        col("data.payload.after.tax_percent").alias("tax_percent"),
        col("data.payload.after.net_profit").alias("net_profit"),
        col("data.payload.after.eps_in_rs").alias("eps_in_rs"),
        col("data.payload.after.dividend_payout_percent").alias("dividend_payout_percent"),
        col("data.payload.source.ts_ms").alias("ts_ms")  # Adding ts_ms field
    )

# Show the schema and some data for debugging
df.printSchema()
df.show()

# Define a window specification to identify the latest row for each unique date
window_spec = Window.partitionBy("date").orderBy(col("ts_ms").desc())

# Deduplicate incoming Kafka data, keeping only the most recent records for each date
df_deduped = df.withColumn("row_number", row_number().over(window_spec)) \
               .filter(col("row_number") == 1) \
               .drop("row_number")

# Show deduplicated data for debugging
df_deduped.show()

# Define PostgreSQL connection properties
postgres_url = "jdbc:postgresql://192.168.56.1:5432/concourse"
postgres_table = "your_sink_table"
postgres_properties = {
    "user": "concourse_user",
    "password": "concourse_pass",
    "driver": "org.postgresql.Driver"
}

# Read the existing data from PostgreSQL to handle updates
existing_df = spark.read \
    .format("jdbc") \
    .option("url", postgres_url) \
    .option("dbtable", postgres_table) \
    .option("user", postgres_properties["user"]) \
    .option("password", postgres_properties["password"]) \
    .option("driver", postgres_properties["driver"]) \
    .load()

# Define the desired column order
column_order = [
    "date",
    "sales",
    "expenses",
    "operating_profit",
    "opm_percent",
    "other_income",
    "interest",
    "depreciation",
    "profit_before_tax",
    "tax_percent",
    "net_profit",
    "eps_in_rs",
    "dividend_payout_percent"
]

# Reorder columns in existing_df and df_deduped to match the desired column order
existing_df = existing_df.select([col(c) for c in column_order])
df_deduped = df_deduped.select([col(c) for c in column_order])

# Combine existing data with deduplicated Kafka data
combined_df = existing_df.alias("existing").join(
    df_deduped.alias("incoming"),
    on="date",
    how="outer"
).select(
    col("incoming.date").alias("date"),
    col("incoming.sales").alias("sales"),
    col("incoming.expenses").alias("expenses"),
    col("incoming.operating_profit").alias("operating_profit"),
    col("incoming.opm_percent").alias("opm_percent"),
    col("incoming.other_income").alias("other_income"),
    col("incoming.interest").alias("interest"),
    col("incoming.depreciation").alias("depreciation"),
    col("incoming.profit_before_tax").alias("profit_before_tax"),
    col("incoming.tax_percent").alias("tax_percent"),
    col("incoming.net_profit").alias("net_profit"),
    col("incoming.eps_in_rs").alias("eps_in_rs"),
    col("incoming.dividend_payout_percent").alias("dividend_payout_percent")
).distinct()

# Show combined data for debugging
combined_df.show()

# Calculate averages for each column
filtered_df = combined_df.filter(~col("date").startswith("TTM"))
average_values = filtered_df.agg(
    avg("sales").alias("avg_sales"),
    avg("expenses").alias("avg_expenses"),
    avg("operating_profit").alias("avg_operating_profit"),
    avg("opm_percent").alias("avg_opm_percent"),
    avg("other_income").alias("avg_other_income"),
    avg("interest").alias("avg_interest"),
    avg("depreciation").alias("avg_depreciation"),
    avg("profit_before_tax").alias("avg_profit_before_tax"),
    avg("tax_percent").alias("avg_tax_percent"),
    avg("net_profit").alias("avg_net_profit"),
    avg("eps_in_rs").alias("avg_eps_in_rs"),
    avg("dividend_payout_percent").alias("avg_dividend_payout_percent")
)

# Add average values as a new row to the DataFrame
average_row_dict = {
    "date": "Average",
    "sales": average_values.select("avg_sales").collect()[0][0],
    "expenses": average_values.select("avg_expenses").collect()[0][0],
    "operating_profit": average_values.select("avg_operating_profit").collect()[0][0],
    "opm_percent": average_values.select("avg_opm_percent").collect()[0][0],
    "other_income": average_values.select("avg_other_income").collect()[0][0],
    "interest": average_values.select("avg_interest").collect()[0][0],
    "depreciation": average_values.select("avg_depreciation").collect()[0][0],
    "profit_before_tax": average_values.select("avg_profit_before_tax").collect()[0][0],
    "tax_percent": average_values.select("avg_tax_percent").collect()[0][0],
    "net_profit": average_values.select("avg_net_profit").collect()[0][0],
    "eps_in_rs": average_values.select("avg_eps_in_rs").collect()[0][0],
    "dividend_payout_percent": average_values.select("avg_dividend_payout_percent").collect()[0][0]
}

# Define the schema for the average row DataFrame
average_row_schema = StructType([
    StructField("date", StringType(), True),
    StructField("sales", DoubleType(), True),
    StructField("expenses", DoubleType(), True),
    StructField("operating_profit", DoubleType(), True),
    StructField("opm_percent", DoubleType(), True),
    StructField("other_income", DoubleType(), True),
    StructField("interest", DoubleType(), True),
    StructField("depreciation", DoubleType(), True),
    StructField("profit_before_tax", DoubleType(), True),
    StructField("tax_percent", DoubleType(), True),
    StructField("net_profit", DoubleType(), True),
    StructField("eps_in_rs", DoubleType(), True),
    StructField("dividend_payout_percent", DoubleType(), True)
])

average_row_df = spark.createDataFrame([average_row_dict], schema=average_row_schema)

# Reorder columns in average_row_df to match the column order of combined_df
average_row_df = average_row_df.select([col(c) for c in column_order])

# Append the averages row to the combined DataFrame
final_df = combined_df.union(average_row_df)

# Write the updated data to PostgreSQL
final_df.write \
    .format("jdbc") \
    .option("url", postgres_url) \
    .option("dbtable", postgres_table) \
    .option("user", postgres_properties["user"]) \
    .option("password", postgres_properties["password"]) \
    .option("driver", postgres_properties["driver"]) \
    .mode("overwrite") \
    .save()

print("Data written to PostgreSQL successfully.")



# from awsglue.context import GlueContext
# from pyspark.context import SparkContext
# from pyspark.sql.functions import col, avg, row_number
# from pyspark.sql.window import Window
# from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
# import time

# # Initialize Spark and Glue contexts
# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session

# # Define Kafka parameters
# kafka_bootstrap_servers = "broker2:29092"
# kafka_topic = "r1.public.reliance_data2"

# # Define the schema of the incoming Kafka messages
# schema = StructType([
#     StructField("payload", StructType([
#         StructField("before", StructType([
#             StructField("date", StringType(), True),
#             StructField("sales", LongType(), True),
#             StructField("expenses", LongType(), True),
#             StructField("operating_profit", LongType(), True),
#             StructField("opm_percent", LongType(), True),
#             StructField("other_income", LongType(), True),
#             StructField("interest", LongType(), True),
#             StructField("depreciation", LongType(), True),
#             StructField("profit_before_tax", LongType(), True),
#             StructField("tax_percent", DoubleType(), True),
#             StructField("net_profit", LongType(), True),
#             StructField("eps_in_rs", DoubleType(), True),
#             StructField("dividend_payout_percent", DoubleType(), True)
#         ])),
#         StructField("after", StructType([
#             StructField("date", StringType(), True),
#             StructField("sales", LongType(), True),
#             StructField("expenses", LongType(), True),
#             StructField("operating_profit", LongType(), True),
#             StructField("opm_percent", LongType(), True),
#             StructField("other_income", LongType(), True),
#             StructField("interest", LongType(), True),
#             StructField("depreciation", LongType(), True),
#             StructField("profit_before_tax", LongType(), True),
#             StructField("tax_percent", DoubleType(), True),
#             StructField("net_profit", LongType(), True),
#             StructField("eps_in_rs", DoubleType(), True),
#             StructField("dividend_payout_percent", DoubleType(), True)
#         ])),
#         StructField("source", StructType([
#             StructField("ts_ms", LongType(), True)  # Extracting the timestamp field
#         ]))
#     ]))
# ])

# # Define PostgreSQL connection properties
# postgres_url = "jdbc:postgresql://192.168.56.1:5432/concourse"
# postgres_table = "your_sink_table"
# postgres_properties = {
#     "user": "concourse_user",
#     "password": "concourse_pass",
#     "driver": "org.postgresql.Driver"
# }

# # Define the desired column order
# column_order = [
#     "date",
#     "sales",
#     "expenses",
#     "operating_profit",
#     "opm_percent",
#     "other_income",
#     "interest",
#     "depreciation",
#     "profit_before_tax",
#     "tax_percent",
#     "net_profit",
#     "eps_in_rs",
#     "dividend_payout_percent"
# ]

# def process_kafka_data():
#     # Read data from Kafka topic
#     df = spark \
#         .read \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
#         .option("subscribe", kafka_topic) \
#         .option("startingOffsets", "earliest") \
#         .load() \
#         .selectExpr("CAST(value AS STRING)") \
#         .selectExpr(f"from_json(value, '{schema.simpleString()}') as data") \
#         .select(
#             col("data.payload.after.date").alias("date"),
#             col("data.payload.after.sales").alias("sales"),
#             col("data.payload.after.expenses").alias("expenses"),
#             col("data.payload.after.operating_profit").alias("operating_profit"),
#             col("data.payload.after.opm_percent").alias("opm_percent"),
#             col("data.payload.after.other_income").alias("other_income"),
#             col("data.payload.after.interest").alias("interest"),
#             col("data.payload.after.depreciation").alias("depreciation"),
#             col("data.payload.after.profit_before_tax").alias("profit_before_tax"),
#             col("data.payload.after.tax_percent").alias("tax_percent"),
#             col("data.payload.after.net_profit").alias("net_profit"),
#             col("data.payload.after.eps_in_rs").alias("eps_in_rs"),
#             col("data.payload.after.dividend_payout_percent").alias("dividend_payout_percent"),
#             col("data.payload.source.ts_ms").alias("ts_ms")  # Adding ts_ms field
#         )

#     # Define a window specification to identify the latest row for each unique date
#     window_spec = Window.partitionBy("date").orderBy(col("ts_ms").desc())

#     # Deduplicate incoming Kafka data, keeping only the most recent records for each date
#     df_deduped = df.withColumn("row_number", row_number().over(window_spec)) \
#                    .filter(col("row_number") == 1) \
#                    .drop("row_number")
#     # Read the existing data from PostgreSQL to handle updates
#     existing_df = spark.read \
#         .format("jdbc") \
#         .option("url", postgres_url) \
#         .option("dbtable", postgres_table) \
#         .option("user", postgres_properties["user"]) \
#         .option("password", postgres_properties["password"]) \
#         .option("driver", postgres_properties["driver"]) \
#         .load()

#     # Reorder columns in existing_df and df_deduped to match the desired column order
#     existing_df = existing_df.select([col(c) for c in column_order])
#     df_deduped = df_deduped.select([col(c) for c in column_order])

#     # Combine existing data with deduplicated Kafka data
#     combined_df = existing_df.alias("existing").join(
#         df_deduped.alias("incoming"),
#         on="date",
#         how="outer"
#     ).select(
#         col("incoming.date").alias("date"),
#         col("incoming.sales").alias("sales"),
#         col("incoming.expenses").alias("expenses"),
#         col("incoming.operating_profit").alias("operating_profit"),
#         col("incoming.opm_percent").alias("opm_percent"),
#         col("incoming.other_income").alias("other_income"),
#         col("incoming.interest").alias("interest"),
#         col("incoming.depreciation").alias("depreciation"),
#         col("incoming.profit_before_tax").alias("profit_before_tax"),
#         col("incoming.tax_percent").alias("tax_percent"),
#         col("incoming.net_profit").alias("net_profit"),
#         col("incoming.eps_in_rs").alias("eps_in_rs"),
#         col("incoming.dividend_payout_percent").alias("dividend_payout_percent")
#     ).distinct()

#     # Calculate averages for each column
#     filtered_df = combined_df.filter(~col("date").startswith("TTM"))
#     average_values = filtered_df.agg(
#         avg("sales").alias("avg_sales"),
#         avg("expenses").alias("avg_expenses"),
#         avg("operating_profit").alias("avg_operating_profit"),
#         avg("opm_percent").alias("avg_opm_percent"),
#         avg("other_income").alias("avg_other_income"),
#         avg("interest").alias("avg_interest"),
#         avg("depreciation").alias("avg_depreciation"),
#         avg("profit_before_tax").alias("avg_profit_before_tax"),
#         avg("tax_percent").alias("avg_tax_percent"),
#         avg("net_profit").alias("avg_net_profit"),
#         avg("eps_in_rs").alias("avg_eps_in_rs"),
#         avg("dividend_payout_percent").alias("avg_dividend_payout_percent")
#     )

#     # Add average values as a new row to the DataFrame
#     average_row_dict = {
#         "date": "Average",
#         "sales": average_values.select("avg_sales").collect()[0][0],
#         "expenses": average_values.select("avg_expenses").collect()[0][0],
#         "operating_profit": average_values.select("avg_operating_profit").collect()[0][0],
#         "opm_percent": average_values.select("avg_opm_percent").collect()[0][0],
#         "other_income": average_values.select("avg_other_income").collect()[0][0],
#         "interest": average_values.select("avg_interest").collect()[0][0],
#         "depreciation": average_values.select("avg_depreciation").collect()[0][0],
#         "profit_before_tax": average_values.select("avg_profit_before_tax").collect()[0][0],
#         "tax_percent": average_values.select("avg_tax_percent").collect()[0][0],
#         "net_profit": average_values.select("avg_net_profit").collect()[0][0],
#         "eps_in_rs": average_values.select("avg_eps_in_rs").collect()[0][0],
#         "dividend_payout_percent": average_values.select("avg_dividend_payout_percent").collect()[0][0]
#     }
#     average_row_df = spark.createDataFrame([average_row_dict])

#     # Reorder columns in average_row_df to match the column order of combined_df
#     average_row_df = average_row_df.select([col(c) for c in column_order])

#     # Append the averages row to the combined DataFrame
#     final_df = combined_df.union(average_row_df)

#     # Write the updated data to PostgreSQL
#     final_df.write \
#         .format("jdbc") \
#         .option("url", postgres_url) \
#         .option("dbtable", postgres_table) \
#         .option("user", postgres_properties["user"]) \
#         .option("password", postgres_properties["password"]) \
#         .option("driver", postgres_properties["driver"]) \
#         .mode("overwrite") \
#         .save()

#     print("Data written to PostgreSQL successfully.")

# # Continuous polling loop
# while True:
#     process_kafka_data()
#     # Sleep for a defined interval before checking for new data
#     time.sleep(5)  # Adjust the interval as needed



# from awsglue.context import GlueContext
# from pyspark.context import SparkContext
# from pyspark.sql.functions import col, avg, row_number
# from pyspark.sql.window import Window
# from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
# import psycopg2
# import time

# # Initialize Spark and Glue contexts
# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session

# # Define Kafka parameters
# kafka_bootstrap_servers = "broker2:29092"
# kafka_topic = "r1.public.reliance_data2"

# # Define the schema of the incoming Kafka messages
# schema = StructType([
#     StructField("payload", StructType([
#         StructField("before", StructType([
#             StructField("date", StringType(), True),
#             StructField("sales", LongType(), True),
#             StructField("expenses", LongType(), True),
#             StructField("operating_profit", LongType(), True),
#             StructField("opm_percent", LongType(), True),
#             StructField("other_income", LongType(), True),
#             StructField("interest", LongType(), True),
#             StructField("depreciation", LongType(), True),
#             StructField("profit_before_tax", LongType(), True),
#             StructField("tax_percent", DoubleType(), True),
#             StructField("net_profit", LongType(), True),
#             StructField("eps_in_rs", DoubleType(), True),
#             StructField("dividend_payout_percent", DoubleType(), True)
#         ])),
#         StructField("after", StructType([
#             StructField("date", StringType(), True),
#             StructField("sales", LongType(), True),
#             StructField("expenses", LongType(), True),
#             StructField("operating_profit", LongType(), True),
#             StructField("opm_percent", LongType(), True),
#             StructField("other_income", LongType(), True),
#             StructField("interest", LongType(), True),
#             StructField("depreciation", LongType(), True),
#             StructField("profit_before_tax", LongType(), True),
#             StructField("tax_percent", DoubleType(), True),
#             StructField("net_profit", LongType(), True),
#             StructField("eps_in_rs", DoubleType(), True),
#             StructField("dividend_payout_percent", DoubleType(), True)
#         ])),
#         StructField("source", StructType([
#             StructField("ts_ms", LongType(), True)  # Extracting the timestamp field
#         ]))
#     ]))
# ])

# # Define PostgreSQL connection properties
# postgres_url = "jdbc:postgresql://192.168.56.1:5432/concourse"
# postgres_table = "your_sink_table"
# postgres_properties = {
#     "user": "concourse_user",
#     "password": "concourse_pass",
#     "driver": "org.postgresql.Driver"
# }

# # Define the desired column order
# column_order = [
#     "date",
#     "sales",
#     "expenses",
#     "operating_profit",
#     "opm_percent",
#     "other_income",
#     "interest",
#     "depreciation",
#     "profit_before_tax",
#     "tax_percent",
#     "net_profit",
#     "eps_in_rs",
#     "dividend_payout_percent"
# ]

# def create_table_if_not_exists():
#     # Connect to PostgreSQL using psycopg2 and create the table if it does not exist
#     connection = psycopg2.connect(
#         host="192.168.56.1",
#         database="concourse",
#         user="concourse_user",
#         password="concourse_pass"
#     )
#     cursor = connection.cursor()

#     # SQL query to create the table if it doesn't exist
#     create_table_sql = f"""
#     CREATE TABLE IF NOT EXISTS {postgres_table} (
#         date VARCHAR,
#         sales BIGINT,
#         expenses BIGINT,
#         operating_profit BIGINT,
#         opm_percent DOUBLE PRECISION,
#         other_income BIGINT,
#         interest BIGINT,
#         depreciation BIGINT,
#         profit_before_tax BIGINT,
#         tax_percent DOUBLE PRECISION,
#         net_profit BIGINT,
#         eps_in_rs DOUBLE PRECISION,
#         dividend_payout_percent DOUBLE PRECISION
#     );
#     """

#     # Execute the query
#     cursor.execute(create_table_sql)
#     connection.commit()

#     # Close the connection
#     cursor.close()
#     connection.close()

# def process_kafka_data():
#     # Read data from Kafka topic
#     df = spark \
#         .read \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
#         .option("subscribe", kafka_topic) \
#         .option("startingOffsets", "earliest") \
#         .load() \
#         .selectExpr("CAST(value AS STRING)") \
#         .selectExpr(f"from_json(value, '{schema.simpleString()}') as data") \
#         .select(
#             col("data.payload.after.date").alias("date"),
#             col("data.payload.after.sales").alias("sales"),
#             col("data.payload.after.expenses").alias("expenses"),
#             col("data.payload.after.operating_profit").alias("operating_profit"),
#             col("data.payload.after.opm_percent").alias("opm_percent"),
#             col("data.payload.after.other_income").alias("other_income"),
#             col("data.payload.after.interest").alias("interest"),
#             col("data.payload.after.depreciation").alias("depreciation"),
#             col("data.payload.after.profit_before_tax").alias("profit_before_tax"),
#             col("data.payload.after.tax_percent").alias("tax_percent"),
#             col("data.payload.after.net_profit").alias("net_profit"),
#             col("data.payload.after.eps_in_rs").alias("eps_in_rs"),
#             col("data.payload.after.dividend_payout_percent").alias("dividend_payout_percent"),
#             col("data.payload.source.ts_ms").alias("ts_ms")  # Adding ts_ms field
#         )

#     # Define a window specification to identify the latest row for each unique date
#     window_spec = Window.partitionBy("date").orderBy(col("ts_ms").desc())

#     # Deduplicate incoming Kafka data, keeping only the most recent records for each date
#     df_deduped = df.withColumn("row_number", row_number().over(window_spec)) \
#                    .filter(col("row_number") == 1) \
#                    .drop("row_number")
#     # Read the existing data from PostgreSQL to handle updates
#     existing_df = spark.read \
#         .format("jdbc") \
#         .option("url", postgres_url) \
#         .option("dbtable", postgres_table) \
#         .option("user", postgres_properties["user"]) \
#         .option("password", postgres_properties["password"]) \
#         .option("driver", postgres_properties["driver"]) \
#         .load()

#     # Reorder columns in existing_df and df_deduped to match the desired column order
#     existing_df = existing_df.select([col(c) for c in column_order])
#     df_deduped = df_deduped.select([col(c) for c in column_order])

#     # Combine existing data with deduplicated Kafka data
#     combined_df = existing_df.alias("existing").join(
#         df_deduped.alias("incoming"),
#         on="date",
#         how="outer"
#     ).select(
#         col("incoming.date").alias("date"),
#         col("incoming.sales").alias("sales"),
#         col("incoming.expenses").alias("expenses"),
#         col("incoming.operating_profit").alias("operating_profit"),
#         col("incoming.opm_percent").alias("opm_percent"),
#         col("incoming.other_income").alias("other_income"),
#         col("incoming.interest").alias("interest"),
#         col("incoming.depreciation").alias("depreciation"),
#         col("incoming.profit_before_tax").alias("profit_before_tax"),
#         col("incoming.tax_percent").alias("tax_percent"),
#         col("incoming.net_profit").alias("net_profit"),
#         col("incoming.eps_in_rs").alias("eps_in_rs"),
#         col("incoming.dividend_payout_percent").alias("dividend_payout_percent")
#     ).distinct()

#     # Calculate averages for each column
#     filtered_df = combined_df.filter(~col("date").startswith("TTM"))
#     average_values = filtered_df.agg(
#         avg("sales").alias("avg_sales"),
#         avg("expenses").alias("avg_expenses"),
#         avg("operating_profit").alias("avg_operating_profit"),
#         avg("opm_percent").alias("avg_opm_percent"),
#         avg("other_income").alias("avg_other_income"),
#         avg("interest").alias("avg_interest"),
#         avg("depreciation").alias("avg_depreciation"),
#         avg("profit_before_tax").alias("avg_profit_before_tax"),
#         avg("tax_percent").alias("avg_tax_percent"),
#         avg("net_profit").alias("avg_net_profit"),
#         avg("eps_in_rs").alias("avg_eps_in_rs"),
#         avg("dividend_payout_percent").alias("avg_dividend_payout_percent")
#     )

#     # Add average values as a new row to the DataFrame
#     average_row_dict = {
#         "date": "Average",
#         "sales": average_values.select("avg_sales").collect()[0][0],
#         "expenses": average_values.select("avg_expenses").collect()[0][0],
#         "operating_profit": average_values.select("avg_operating_profit").collect()[0][0],
#         "opm_percent": average_values.select("avg_opm_percent").collect()[0][0],
#         "other_income": average_values.select("avg_other_income").collect()[0][0],
#         "interest": average_values.select("avg_interest").collect()[0][0],
#         "depreciation": average_values.select("avg_depreciation").collect()[0][0],
#         "profit_before_tax": average_values.select("avg_profit_before_tax").collect()[0][0],
#         "tax_percent": average_values.select("avg_tax_percent").collect()[0][0],
#         "net_profit": average_values.select("avg_net_profit").collect()[0][0],
#         "eps_in_rs": average_values.select("avg_eps_in_rs").collect()[0][0],
#         "dividend_payout_percent": average_values.select("avg_dividend_payout_percent").collect()[0][0]
#     }
#     average_row_df = spark.createDataFrame([average_row_dict])

#     # Reorder columns in average_row_df to match the column order of combined_df
#     average_row_df = average_row_df.select([col(c) for c in column_order])

#     # Append the averages row to the combined DataFrame
#     final_df = combined_df.union(average_row_df)

#     # Write the updated data to PostgreSQL
#     final_df.write \
#         .format("jdbc") \
#         .option("url", postgres_url) \
#         .option("dbtable", postgres_table) \
#         .option("user", postgres_properties["user"]) \
#         .option("password", postgres_properties["password"]) \
#         .option("driver", postgres_properties["driver"]) \
#         .mode("overwrite") \
#         .save()

#     print("Data written to PostgreSQL successfully.")

# # Ensure the table exists before processing data
# create_table_if_not_exists()

# # Continuous polling loop
# while True:
#     process_kafka_data()
#     # Sleep for a defined interval before checking for new data
#     time.sleep(5)  # Adjust the interval as needed
