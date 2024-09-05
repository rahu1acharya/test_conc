from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, avg, row_number, coalesce
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.sparkContext.setLogLevel("ERROR")

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
        col("data.payload.source.ts_ms").alias("ts_ms"),  # Adding ts_ms field
        col("data.payload.before.date").alias("before_date")  # Adding before_date for deletions
    )

# Show the schema and some data for debugging
df.printSchema()
df.show()

# Define a window specification to identify the latest row for each unique date
window_spec = Window.partitionBy("date").orderBy(col("ts_ms").desc())

# Remove null rows resulting from tombstones
df_filtered = df.filter(col("date").isNotNull())

# Deduplicate incoming Kafka data, keeping only the most recent records for each date
df_deduped = df_filtered.withColumn("row_number", row_number().over(window_spec)) \
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

# Extract records to be deleted
# Extract records that have been deleted (i.e., "before" is not null and "after" is null)
deleted_df = df.filter(col("before_date").isNotNull() & col("date").isNull()).select(
    col("before_date").alias("date")  # Select the "before.date" field as the "date" column
).distinct()


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
    coalesce(col("incoming.sales"), col("existing.sales")).alias("sales"),
    coalesce(col("incoming.expenses"), col("existing.expenses")).alias("expenses"),
    coalesce(col("incoming.operating_profit"), col("existing.operating_profit")).alias("operating_profit"),
    coalesce(col("incoming.opm_percent"), col("existing.opm_percent")).alias("opm_percent"),
    coalesce(col("incoming.other_income"), col("existing.other_income")).alias("other_income"),
    coalesce(col("incoming.interest"), col("existing.interest")).alias("interest"),
    coalesce(col("incoming.depreciation"), col("existing.depreciation")).alias("depreciation"),
    coalesce(col("incoming.profit_before_tax"), col("existing.profit_before_tax")).alias("profit_before_tax"),
    coalesce(col("incoming.tax_percent"), col("existing.tax_percent")).alias("tax_percent"),
    coalesce(col("incoming.net_profit"), col("existing.net_profit")).alias("net_profit"),
    coalesce(col("incoming.eps_in_rs"), col("existing.eps_in_rs")).alias("eps_in_rs"),
    coalesce(col("incoming.dividend_payout_percent"), col("existing.dividend_payout_percent")).alias("dividend_payout_percent")
).distinct()

# Show combined data for debugging
combined_df.show()

# Filter out deleted records from combined_df
final_df = combined_df.join(
    deleted_df,
    on="date",
    how="left_anti"  # Keep only records that are not in deleted_df
)

# Calculate averages for each column
filtered_df = final_df.filter(~col("date").startswith("TTM"))
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

average_row_df = spark.createDataFrame([average_row_dict])

# Reorder columns in average_row_df to match final_df
average_row_df = average_row_df.select([col(c) for c in column_order])

# Union the average row with the existing and incoming data
final_df = final_df.union(average_row_df)

# Remove rows where all columns are null from final_df
final_df = final_df.filter(
    col("date").isNotNull() &
    col("sales").isNotNull() &
    col("expenses").isNotNull() &
    col("operating_profit").isNotNull() &
    col("opm_percent").isNotNull() &
    col("other_income").isNotNull() &
    col("interest").isNotNull() &
    col("depreciation").isNotNull() &
    col("profit_before_tax").isNotNull() &
    col("tax_percent").isNotNull() &
    col("net_profit").isNotNull() &
    col("eps_in_rs").isNotNull() &
    col("dividend_payout_percent").isNotNull()
)

final_df.show()

# Write the final DataFrame to PostgreSQL
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
