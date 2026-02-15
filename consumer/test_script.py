# test_kafka_detailed.py
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient
import sys

print("Testing Kafka connection...")

# Test 1: Admin client
try:
    print("\n1. Testing admin connection...")
    admin = KafkaAdminClient(
        bootstrap_servers=['localhost:9092'],
        request_timeout_ms=10000
    )
    print("✅ Admin client connected!")
    admin.close()
except Exception as e:
    print(f"❌ Admin client failed: {e}")
    sys.exit(1)

# Test 2: List topics
try:
    print("\n2. Listing topics...")
    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        request_timeout_ms=10000,
        consumer_timeout_ms=5000
    )
    topics = consumer.topics()
    print(f"✅ Available topics: {topics}")
    consumer.close()
except Exception as e:
    print(f"❌ Failed to list topics: {e}")
    sys.exit(1)

print("\n✅ All tests passed!")