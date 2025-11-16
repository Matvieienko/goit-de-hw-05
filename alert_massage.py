from kafka import KafkaConsumer
from configs import kafka_config, my_name
import json

# Назви топіків для читання алертів
temperature_alerts_topic = f"{my_name}_temperature_alerts"
humidity_alerts_topic = f"{my_name}_humidity_alerts"

# Створення Consumer для читання сповіщень
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=f"{my_name}_alerts_consumer"
)

# Підписуємося на два топіки
consumer.subscribe([temperature_alerts_topic, humidity_alerts_topic])

print(f"Subscribed to alert topics:")
print(f" - Temperature alerts: {temperature_alerts_topic}")
print(f" - Humidity alerts: {humidity_alerts_topic}")
print("Waiting for alerts...\n")

try:
    for message in consumer:
        topic = message.topic
        alert = message.value

        print(f"[ALERT from {topic}]")
        print(alert)
        print("-" * 50)

except KeyboardInterrupt:
    print("Stopped by user (Ctrl+C).")

except Exception as e:
    print(f"Error: {e}")

finally:
    consumer.close()
    print("Alert consumer closed.")
