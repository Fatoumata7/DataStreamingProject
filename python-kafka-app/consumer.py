import json
from kafka import KafkaConsumer

# Configuration
BROKER = 'localhost:9092'
TOPIC = 'fraud-alerts'

def consume_fraud_alerts():
    """
    Consumes fraud alerts results from Kafka and displays them.
    """
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )

    print("Waiting for fraud alerts results...")
    for message in consumer:
        result = message.value
        print(f"Fraud Alerts Result: {result}")


if __name__ == "__main__":
    consume_fraud_alerts()
