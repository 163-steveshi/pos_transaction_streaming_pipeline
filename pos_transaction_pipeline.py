import os
from py4j.protocol import Py4JError
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    when,
    lit,
    array,
    expr,
    concat,
    filter as sql_filter,
    unix_timestamp,
    lit as F_lit,
    size,
    coalesce,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)
from dotenv import load_dotenv

from reference_data import (
    CHANNELS,
    COUNTRIES,
    PAYMENT_TYPES,
    STORE_IDS,
)
from delta.tables import DeltaTable

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
CHECKPOINT_PATH = f"{VOLUME_PATH}/checkpoints/aiven_s3_local/bronze"

# TARGET DATABASE TABLE
BRONZE_TABLE_PATH = os.getenv("BRONZE_TABLE_PATH")
SILVER_CLEAN_TABLE_PATH = os.getenv("SILVER_CLEAN_TABLE_PATH", "")
SILVER_LATE_TABLE_PATH = os.getenv("SILVER_LATE_TABLE_PATH")
BRONZE_DLQ_TABLE_PATH = os.getenv("BRONZE_DLQ_TABLE_PATH")
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

bronze_query = (
    processed_stream.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")  # solve schema evolution
    .trigger(processingTime="60 seconds")
    .start(BRONZE_TABLE_PATH)
)


# TODO: write a dag that monitor the running of the script, and configure retry and  email the user after the all retries are failed


# usually for real time straeming Define the watermark threshold: Tell Spark to discard any data arriving > 10 minutes late
# this is for data such as IOT, browser events, etc that can tolerant the late arrive is discarded
# or the later arrive will not impact the analyze result at all(e.g a security window that's already closed and already been evaluated — recomputing it retroactively doesn't help you prevent anything. )

# watermarked_stream = processed_stream.withWatermark("event_timestamp", "10 minutes")

# for finance data with revenue needed: you need to actuall save the data into the backfill table


bronze_stream = spark.readStream.format("delta").load(
    BRONZE_TABLE_PATH
)  # change to load(BRONZE_PATH)

processed_stream_with_lateness = (
    bronze_stream.withColumn("event_time_ts", col("event_time").cast("timestamp"))
    .withColumn(
        "ingestion_delay_seconds",
        when(
            col("event_time_ts").isNotNull(),
            unix_timestamp(col("ingested_time")) - unix_timestamp(col("event_time_ts")),
        ),
    )
    .withColumn(
        "is_late",
        when(
            col("ingestion_delay_seconds").isNotNull(),
            col("ingestion_delay_seconds") > 3600,
        ).otherwise(False),
    )
)


def rotute_and_upsert_batch(micro_batch_df, batch_id):
    print(f"Processing micro-batch ID: {batch_id}")
    # STEP 1 get all null column record
    required_columns = [
        "transaction_id",
        "item_sequence",
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
    ).withColumn(
        "error_reason", expr("filter(error_reason, x -> x is not null)")
    )  # remove the null in the error reason array
    casted_check_df = (
        null_check_df.withColumn(
            "item_unit_price_casted", expr("try_cast(item_unit_price AS DECIMAL(10,2))")
        )
        .withColumn(
            "item_quantity_casted", expr("try_cast(item_quantity AS DECIMAL(10,2))")
        )
        .withColumn(
            "discount_amount_casted", expr("try_cast(discount_amount AS DECIMAL(10,2))")
        )
        .withColumn("tax_amount_casted", expr("try_cast(tax_amount AS DECIMAL(10,2))"))
        .withColumn("item_sequence_casted", expr("try_cast(item_sequence AS INT)"))
        .withColumn("event_time_casted", expr("try_cast(event_time AS TIMESTAMP)"))
        .withColumn(
            "error_reason",
            concat(
                coalesce(col("error_reason"), array().cast("array<string>")),
                array(
                    *[
                        when(
                            col(raw_c).isNotNull() & col(casted_c).isNull(),
                            lit(f"INVALID_FORMAT_{raw_c.upper()}"),
                        )
                        for raw_c, casted_c in [
                            ("item_unit_price", "item_unit_price_casted"),
                            ("item_quantity", "item_quantity_casted"),
                            ("event_time", "event_time_casted"),
                            ("item_sequence", "item_sequence_casted"),
                            ("discount_amount", "discount_amount_casted"),
                            ("tax_amount", "tax_amount_casted"),
                        ]
                    ]
                ),
            ),
        )
        .withColumn("error_reason", expr("filter(error_reason, x -> x is not null)"))
    )

    MAX_QUANTITY = 200  # No single customer buys 500 of one item in a normal line
    MAX_UNIT_PRICE = 200.0  # Upper limit for your pos item price

    # TODO: google and search a way to refact the code for readability
    business_rule_check_df = casted_check_df.withColumn(
        "error_reason",
        concat(
            col("error_reason"),
            array(
                when(
                    (col("item_sequence_casted") <= 0),
                    lit("INVALID_RANGE_ITEM_SEQUENCE"),
                ),
                when(
                    (
                        (col("item_quantity_casted") <= 0)
                        | (col("item_quantity_casted") > MAX_QUANTITY)
                    ),
                    lit("INVALID_RANGE_ITEM_QUANTITY"),
                ),
                when(
                    (
                        (col("item_unit_price_casted") <= 0)
                        | (col("item_unit_price_casted") > MAX_UNIT_PRICE)
                    ),
                    lit("INVALID_RANGE_ITEM_UNIT_PRICE"),
                ),
                when(col("tax_amount_casted") < 0, lit("INVALID_RANGE_TAX_AMOUNT")),
                when(
                    col("discount_amount_casted") < 0,
                    lit("INVALID_RANGE_DISCOUNT_AMOUNT"),
                ),
                when(
                    col("event_time_casted") > expr("current_timestamp()"),
                    lit("INVALID_FUTURE_EVENT_TIME"),
                ),
                when(
                    col("event_time_casted")
                    == expr("timestamp('1970-01-01 00:00:00')"),
                    lit("EPOCH_RESET_TERMINAL_REBOOT"),
                ),
                when(
                    col("customer_id").contains("\ufffd")
                    | (
                        col("customer_id").isNotNull()
                        & ~col("customer_id").rlike(r"^CUST-\d{5}$")
                    ),
                    lit("GARBLED_CUSTOMER_ID"),
                ),
                when(
                    col("sku").contains("\ufffd") | ~col("sku").rlike(r"^\d{13}$"),
                    lit("GARBLED_SKU"),
                ),
                when(
                    col("store_id").contains("\ufffd")
                    | ~col("store_id").isin(STORE_IDS),
                    lit("GARBLED_STORE_ID"),
                ),
                when(
                    col("terminal_id").contains("\ufffd")
                    | ~col("terminal_id").rlike(r"^TERM-\d{2}$"),
                    lit("GARBLED_TERMINAL_ID"),
                ),
                when(
                    col("payment_type").isNotNull()
                    & ~col("payment_type").isin(PAYMENT_TYPES),
                    lit("INVALID_ENUM_PAYMENT_TYPE"),
                ),
                when(
                    col("channel").isNotNull() & ~col("channel").isin(CHANNELS),
                    lit("INVALID_ENUM_CHANNEL"),
                ),
                when(
                    col("country").isNotNull() & ~col("country").isin(COUNTRIES),
                    lit("INVALID_ENUM_COUNTRY"),
                ),
            ),
        ),
    ).withColumn("error_reason", expr("filter(error_reason, x -> x is not null)"))

    quarantine_df = (
        business_rule_check_df.filter(size("error_reason") > 0)
        .select(
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
            "ingested_time",
            "error_reason",
        )
        .withColumn("is_corrupted", lit(True))
    )

    valid_df = business_rule_check_df.filter(
        col("error_reason").isNull() | (size("error_reason") == 0)
    ).select(
        "transaction_id",
        "sku",
        "payment_type",
        "currency",
        "country",
        "channel",
        "store_id",
        "terminal_id",
        "ingested_time",
        "is_late",
        "customer_id",
        col("item_unit_price_casted").alias("item_unit_price"),
        col("item_quantity_casted").alias("item_quantity"),
        col("event_time_casted").alias("event_time"),
        col("item_sequence_casted").alias("item_sequence"),
        col("discount_amount_casted").alias("discount_amount"),
        col("tax_amount_casted").alias("tax_amount"),
        "ingestion_delay_seconds",
    )

    late_df = valid_df.filter(col("is_late")).select(
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
        "ingested_time",
        "is_late",
        "ingestion_delay_seconds",
    )

    ontime_df = valid_df.filter(~col("is_late")).select(
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
        "ingested_time",
    )
    # # TODO: fix the silver on time df is has zero record
    # quarantine_df.show(5)
    # ontime_df.show(5)
    # late_df.show(5)
    q_count = quarantine_df.count()
    l_count = late_df.count()
    o_count = ontime_df.count()
    print(f"batch {batch_id}: quarantine={q_count}, late={l_count}, ontime={o_count}")

    quarantine_df.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).save(BRONZE_DLQ_TABLE_PATH)

    late_df.write.format("delta").mode("append").option("mergeSchema", "true").save(
        SILVER_LATE_TABLE_PATH
    )

    # # BELOW COMMENT OUT ONLY Work when script executed inside the unicatalog
    # ontime_df.sparkSession.sql(f"""
    #         MERGE INTO {TARGET_DB_TABLE} AS target
    #         USING incoming_batch_view AS source
    #         ON target.transaction_id = source.transaction_id
    #         WHEN MATCHED THEN
    #             UPDATE SET *
    #         WHEN NOT MATCHED THEN
    #             INSERT *
    #     """)
    # print(f"  Successfully merged clean records into {TARGET_DB_TABLE}")

    if DeltaTable.isDeltaTable(micro_batch_df.sparkSession, SILVER_CLEAN_TABLE_PATH):
        target = DeltaTable.forPath(
            micro_batch_df.sparkSession, SILVER_CLEAN_TABLE_PATH
        )
        (
            target.alias("target")
            .merge(
                ontime_df.alias("source"),
                "target.transaction_id = source.transaction_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # first batch ever — table doesn't exist yet, create it directly
        ontime_df.write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).save(SILVER_CLEAN_TABLE_PATH)


silver_query = (
    processed_stream_with_lateness.writeStream.foreachBatch(
        rotute_and_upsert_batch
    )  # Routes to late table, dlq, and regular cleaned table
    .option("checkpointLocation", f"{VOLUME_PATH}/checkpoints/aiven_s3_local/silver")
    # .trigger(availableNow=True)
    .trigger(processingTime="60 seconds")
    .start()
)


try:
    while bronze_query.isActive or silver_query.isActive:
        if bronze_query.isActive:
            bronze_query.awaitTermination(
                5
            )  # returns after 5s, or immediately if stopped
        if silver_query.isActive:
            silver_query.awaitTermination(5)
except (KeyboardInterrupt, Py4JError):
    print("\nShutdown signal received — stopping streaming queries gracefully...")
finally:
    if bronze_query.isActive:
        print("Stopping bronze_query (waiting for current micro-batch to finish)...")
        bronze_query.stop()
    if silver_query.isActive:
        print("Stopping silver_query (waiting for current micro-batch to finish)...")
        silver_query.stop()
    print("Stopping Spark...")
    spark.stop()
    print("Shutdown complete.")
