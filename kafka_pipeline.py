from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, when, lit, array, expr, concat, filter as sql_filter,unix_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)
from operator import and_

# Define the volume path
VOLUME_PATH = "/Volumes/dbs_study/landing/kafaka_use/certs/"
CA_PATH = f"{VOLUME_PATH}/ca.pem"
CERT_PATH = f"{VOLUME_PATH}/service.cert"
KEY_PATH = f"{VOLUME_PATH}/service.key"

# State Management Offsets Path
CHECKPOINT_PATH = f"{VOLUME_PATH}/checkpoints/aiven_pos"

# TARGET DATABASE TABLE
TARGET_DB_TABLE = "dbs_external_another.bronze.raw_fact_transactions"


# Initialize Databricks Spark Session
spark = SparkSession.builder.appName("AivenKafkaDirectStreamingPEM").getOrCreate()
# use this secret scope in the paid edtion, seem not support in the free edition
# KAFKA_BOOTSTRAP_SERVERS = dbutils.secrets.get(scope="pos_transaction_env_var", key="KAFKA_BOOTSTRAP_SERVERS")
# KAFKA_TOPIC = dbutils.secrets.get(scope="pos_transaction_env_var", key="KAFKA_TOPIC")
KAFKA_BOOTSTRAP_SERVERS = "kafka-2eb02e05-kafka202607.e.aivencloud.com:14687"
KAFKA_TOPIC = "pos-transaction-event"


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
    # .trigger(processingTime="60 seconds") # not allowed in the free edition
    .trigger(availableNow=True)
    .toTable(TARGET_DB_TABLE)
)

query.awaitTermination()  # block existed

# usually for real time straeming Define the watermark threshold: Tell Spark to discard any data arriving > 10 minutes late
# this is for data such as IOT, browser events, etc that can tolerant the late arrive is discarded
# or the later arrive will not impact the analyze result at all(e.g a security window that's already closed and already been evaluated — recomputing it retroactively doesn't help you prevent anything. )

# watermarked_stream = processed_stream.withWatermark("event_timestamp", "10 minutes")

# for finance data with revenue needed: you need to actuall save the data into the backfill table
processed_stream_with_lateness = processed_stream.withColumn(
    "is_late",
    (unix_timestamp(current_timestamp()) - unix_timestamp(col("event_timestamp")))
    > (10 * 60),  # 10 min threshold
)


def upsert_to_clean(micro_batch_df, batch_id):
    print(f"Processing micro-batch ID: {batch_id}")
    # STEP 1 get all null column record
    required_columns = [
        "transaction_id",
        "item_sequence",
        "customer_id",
        "event_time",
        "sku",
        "item_unit_price",
        "item_quantity",
        "discount_amount",
        "tax_amount",
        "payment_type",
        "currency",
        "country",
        "channel",
        "store_id",
        "terminal_id",
    ]
    null_check_df = micro_batch_df.withColumn(
        "error_reason",
        array(
            *[
                when(col(c).isNull(), lit(f"MISSING_{c.upper()}"))
                for c in required_columns
            ]
        ),
    ).withColumn("error_reason", expr("filter(error_reason, x -> x is not null)"))

    casted_check_df = (
        null_check_df.withColumn(
            "item_unit_price", expr("try_cast(raw_item_unit_price AS DECIMAL(10,2))")
        )
        .withColumn(
            "item_quantity", expr("try_cast(raw_item_quantity AS DECIMAL(10,2))")
        )
        .withColumn("event_time", expr("try_cast(raw_event_time AS TIMESTAMP)"))
        # ... other casts ...
        .withColumn(
            "error_reason",
            concat(
                col("error_reason"),
                array(
                    *[
                        when(
                            col(raw_c).isNotNull() & col(casted_c).isNull(),
                            lit(f"INVALID_FORMAT_{casted_c.upper()}"),
                        )
                        for raw_c, casted_c in [
                            ("raw_item_unit_price", "item_unit_price"),
                            ("raw_item_quantity", "item_quantity"),
                            ("raw_event_time", "event_time"),
                        ]
                    ]
                ),
            ),
        )
        .withColumn("error_reason", expr("filter(error_reason, x -> x is not null)"))
    )
