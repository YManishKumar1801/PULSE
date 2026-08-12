from kafka import KafkaConsumer
import json

KAFKA_TOPIC = "pulse-youtube"
KAFKA_BROKER = "localhost:9092"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",  
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print(f"Listening on Kafka topic '{KAFKA_TOPIC}'... (Ctrl+C to stop)\n")

for message in consumer:
    data = message.value
    print(f"[{data['keyword']}] {data['title']}")
    print(f"   Channel: {data['channel']} | Comments received: {len(data['comments'])}")
    print()