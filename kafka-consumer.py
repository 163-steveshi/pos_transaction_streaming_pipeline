import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from dotenv import load_dotenv

load_dotenv()


# Kafka Broker Details
KAFKA_BOOTSTRAP_SERVERS = "kafka-dbs-jobready123-a548.d.aivencloud.com:13858"
KAFKA_TOPIC = "customer-web-behaviors"

# Define the POSIX path to your uploaded Volume files
CERT_DIR = os.getenv("KAFKA_CERT_DIR")
CA_FILE = f"{CERT_DIR}/ca.pem"
CERT_FILE = f"{CERT_DIR}/service.cert"
KEY_FILE = f"{CERT_DIR}/service.key"

VOLUME_PATH = ""
# State Management Offsets Path
CHECKPOINT_PATH = f"{VOLUME_PATH}/checkpoints/aiven"

# TARGET DATABASE TABLE
TARGET_DB_TABLE = ""

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
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    )
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("fs.s3a.connection.maximum", "100")
    .config("fs.s3a.threads.max", "20")
    .getOrCreate()
)

# Explicitly define schema for incoming JSON payloads
pos_transaction_schema = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("item_sequence", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("timestamp", StringType(), True),
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
)
