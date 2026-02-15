from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

producer = KafkaProducer(bootstrap_servers='localhost:9092')

producer.send("test-topic", b"Hello, Kafka!")
print("Connected to Kafka!")
