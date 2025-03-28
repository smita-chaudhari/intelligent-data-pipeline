from kafka import KafkaProducer
import csv
import time
import json

# Kafka settings
KAFKA_TOPIC = 'transactions'
KAFKA_SERVER = 'localhost:9092'

# Set up Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Read CSV and send to Kafka
with open('sample_transactions.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        producer.send(KAFKA_TOPIC, value=row)
        print(f"Sent: {row}")
        time.sleep(0.001)  # Simulate real-time stream (1 ms delay)

producer.flush()
producer.close()
print("✅ Done sending messages to Kafka.")

