import json
import random
import time
import os

from faker import Faker
from kafka import KafkaProducer
from data_gen_faker import generate_pos_record
from dotenv import load_dotenv

# Initialize Faker for generating random customer metadata
fake = Faker()
load_dotenv()
# --- AIVEN KAFKA CONFIG URATION ---
AIVEN_BOOTSTRAP_SERVER = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC_NAME = os.environ.get("KAFKA_TOPIC", "pos-transaction-event")

# Paths to your downloaded Aiven credentials
CERT_DIR = os.getenv("KAFKA_CERT_DIR")
CA_FILE = f"{CERT_DIR}/ca.pem"
CERT_FILE = f"{CERT_DIR}/service.cert"
KEY_FILE = f"{CERT_DIR}/service.key"

print("Initializing Kafka Producer connecting to Aiven...")
producer = KafkaProducer(
    bootstrap_servers=AIVEN_BOOTSTRAP_SERVER,
    security_protocol="SSL",
    ssl_cafile=CA_FILE,
    ssl_certfile=CERT_FILE,
    ssl_keyfile=KEY_FILE,
    # Modified value_serializer: If the data arriving is already bytes (malformed text),
    # we pass it directly; otherwise, we encode it as valid JSON.
    value_serializer=lambda v: (
        v if isinstance(v, bytes) else json.dumps(v).encode("utf-8")
    ),
    # Key is serialized as bytes so Kafka's partitioner can hash it
    key_serializer=lambda k: k.encode("utf-8") if k else None,
)

print(producer.bootstrap_connected())
# --- MAIN STREAM LOOP ---
print(f"Starting event generation. Streaming to topic: '{TOPIC_NAME}'...")
try:
    while True:
        # 1. Generate base payload
        event, is_noise = generate_pos_record()
        # since we only has two cluster should banlance them with division of transaction id
        routing_key = event["transaction_id"]

        # 3. Fire the event into your 2-partition topic
        future = producer.send(TOPIC_NAME, key=routing_key, value=event)
        #  wait up to 10 seconds for Kafka's broker to actually acknowledge the message. Once acknowledged, it returns a RecordMetadata object containing:

        # .partition — which of the topic's partitions the message landed on
        # .offset — the message's position within that partition
        # block next send for learning
        # althrough real time people read future in the batch format
        record_metadata = future.get(timeout=10)

        # Output log with explicit visual markers for toxic records
        if is_noise:
            print(
                f"[TOXIC REQ] Sent -> Key: {routing_key.upper():11} | Partition: {record_metadata.partition} | Offset: {record_metadata.offset}"
            )
        else:
            print(
                f"[{event['event_time']}] Sent -> Key: {routing_key.upper():11} | Partition: {record_metadata.partition} | Offset: {record_metadata.offset}"
            )

        # Simulate natural usage pace
        # hence poroducing maybe 1–3 messages/second, forever. like real time streaming
        time.sleep(random.uniform(0.2, 1.5))

except KeyboardInterrupt:
    print("\nStopping data generator...")
finally:
    print("Flushing and closing Kafka producer connection...")
    producer.flush()
    producer.close()
    print("Producer safely shutdown.")
