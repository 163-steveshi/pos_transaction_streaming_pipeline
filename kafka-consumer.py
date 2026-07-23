import os
import signal
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)
from dotenv import load_dotenv

load_dotenv()
# TODO:only used in wsl remove for other env
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_DRIVER_HOST"] = "127.0.0.1"
# Kafka Broker Details
KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC")

# Define the POSIX path to your uploaded Volume files
CERT_DIR = os.getenv("KAFKA_CERT_DIR")
CA_PATH = f"{CERT_DIR}/ca.pem"
CERT_PATH = f"{CERT_DIR}/service.cert"
KEY_PATH = f"{CERT_DIR}/service.key"

VOLUME_PATH = os.getenv("S3_VOLUME_PATH")
# State Management Offsets Path
CHECKPOINT_PATH = f"{VOLUME_PATH}/checkpoints/aiven"

# TARGET DATABASE TABLE
TARGET_DB_TABLE_PATH = os.getenv("TARGET_DB_TABLE_PATH")

# S3 credentials from local env variables
AWS_S3_ACCESS_KEY = os.environ["AWS_S3_ACCESS_KEY"]
AWS_S3_SECRET_KEY = os.environ["AWS_S3_SECRET_KEY"]
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET_PATH = os.environ["S3_BUCKET_PATH"]


# Initialize Databricks Spark Session
spark = (
    SparkSession.builder.appName("AivenKafkaDirectStreamingPEM")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.access.key", AWS_S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_S3_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,"
        "io.delta:delta-spark_2.12:3.2.0",
    )
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.maximum", "100")  # see note below
    .config("spark.hadoop.fs.s3a.threads.max", "20")  # see note below
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)
# Explicitly define schema for incoming JSON payloads
pos_transaction_schema = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("item_sequence", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("sku", StringType(), True),
        StructField("item_unit_price", StringType(), True),
        StructField("item_quantity", StringType(), True),
        StructField("discount_amount", StringType(), True),
        StructField("tax_amount", StringType(), True),
        StructField("payment_type", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("country", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("terminal_id", StringType(), True),
    ]
)


print("Establishing SSL handshake with Aiven cluster using Volume certs...")

# Stream directly from Aiven Kafka Topic using PEM configurations
raw_kafka_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("kafka.security.protocol", "SSL")
    .option("kafka.ssl.truststore.type", "PEM")
    .option("kafka.ssl.truststore.certificates", open(CA_PATH).read())
    .option("kafka.ssl.keystore.type", "PEM")
    .option("kafka.ssl.keystore.certificate.chain", open(CERT_PATH).read())
    .option("kafka.ssl.keystore.key", open(KEY_PATH).read())
    .option("startingOffsets", "latest")
    .load()
)

# Deserialize binary value payload into structured columns
processed_stream = (
    raw_kafka_stream.selectExpr("CAST(value AS STRING) as json_payload")
    .select(from_json(col("json_payload"), pos_transaction_schema).alias("data"))
    .select("data.*")
    .withColumn("ingested_time", current_timestamp())
)

# BELOW LOGIC NEED TO BE TRIGGERED with valid external table created in the S3

query = (
    processed_stream.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")  # solve schema evolution
    .trigger(processingTime="60 seconds")
    .start(TARGET_DB_TABLE_PATH)
)


def shutdown_handler(sig, frame):
    print("Stopping streaming query gracefully...")
    if query:
        query.stop()
    spark.stop()


signal.signal(signal.SIGINT, shutdown_handler)
query.awaitTermination()  # block existed

# TODO: write a dag that monitor the running of the script, and configure retry and  email the user after the all retries are failed
