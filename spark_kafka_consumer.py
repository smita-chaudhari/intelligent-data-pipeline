from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# Schema
schema = StructType() \
    .add("transaction_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("region", StringType()) \
    .add("security_type", StringType()) \
    .add("amount", StringType())

# Create SparkSession
spark = SparkSession.builder \
    .appName("KafkaToSnowflakeETL") \
    .config("spark.jars", "jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,jars/kafka-clients-3.5.0.jar,jars/snowflake-jdbc-3.13.18.jar,jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar") \
    .getOrCreate()

# Kafka → Streaming DF
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
parsed_df = df_kafka.selectExpr("CAST(value AS STRING)") \
    .withColumn("data", from_json(col("value"), schema)) \
    .select("data.*") \
    .dropDuplicates(["transaction_id"])

# ❄️ Snowflake Config
SNOWFLAKE_OPTIONS = {
    "sfURL": "HPB93921.us-west-2.snowflakecomputing.com",
    "sfUser": "smitamchaudhari",
    "sfPassword": "Snowflake@2024",
    "sfDatabase": "FINANCE_DB",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
    "dbtable": "FRAUD_TRANSACTIONS_CLEAN"
}

def write_to_snowflake(batch_df, batch_id):
    print(f"🚀 Writing batch {batch_id} with {batch_df.count()} rows to Snowflake")
    batch_df.write \
        .format("snowflake") \
        .options(**SNOWFLAKE_OPTIONS) \
        .mode("append") \
        .save()

# Stream to Snowflake
query = parsed_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_snowflake) \
    .start()

query.awaitTermination()

