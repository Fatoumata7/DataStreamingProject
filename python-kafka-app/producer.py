import json
import random
import time
from datetime import datetime, timedelta
from faker import Faker
from kafka import KafkaProducer


BROKER = 'localhost:9092'
TOPIC = 'transactions'

fake = Faker()
ref_start_time = datetime(2025, 6, 1, 0, 0, 0)
i = 0


def generate_transaction(start_time, delta_seconds, tx_id):
    timestamp = start_time + timedelta(seconds=delta_seconds)
    return {
        "user_id": f"u{random.randint(1000, 1100)}",
        "transaction_id": f"t-{tx_id:07}",
        "amount": round(random.uniform(0.0, 5000.0), 2),
        "currency": random.choice(["EUR", "USD", "GBP"]),
        "timestamp": timestamp.isoformat(),
        "location": fake.city(), 
        "method": random.choice(["credit_card", "debit_card", "paypal", "crypto"])
    }

def produce_transactions(min_tps=10, max_tps=100):
    """
    Produces between <min_tps> and <max_tps> transactions by second
    """
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    global i
    try:
        while True:
            tps = random.randint(min_tps, max_tps)
            for _ in range(tps):
                tx = generate_transaction(ref_start_time, i, i)
                producer.send(TOPIC, value=tx)
                #print(f"Sent to Kafka: {tx}")
                i += 1
            producer.flush()
            time.sleep(1)
            print(f"Adding {tps} transactions -> Total of {i} transactions produced", end="\r")
    except KeyboardInterrupt:
        print("\nProducer stopped.")
    finally:
        producer.close()

if __name__ == "__main__":
    produce_transactions()