from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config, my_name
import json

building_topic = f"{my_name}_building_sensors"
temperature_alerts_topic = f"{my_name}_temperature_alerts"
humidity_alerts_topic = f"{my_name}_humidity_alerts"

# Consumer для читання даних з building_sensors
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',   # читаємо з початку
    enable_auto_commit=True,
    group_id=f"{my_name}_building_processor"  # ідентифікатор групи споживачів
)

# Producer для відправки алертів у відповідні топіки
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Підписуємося на топік з даними датчиків
consumer.subscribe([building_topic])

print(f"Subscribed to '{building_topic}'")
print(f"Temperature alerts topic: '{temperature_alerts_topic}'")
print(f"Humidity alerts topic:    '{humidity_alerts_topic}'")

try:
    for message in consumer:
        data = message.value

        sensor_id = data.get("sensor_id")
        timestamp = data.get("timestamp")
        temperature = data.get("temperature")
        humidity = data.get("humidity")

        print(f"Received from {building_topic}: {data}")

        # Перевірка температури
        if temperature is not None and temperature > 40:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "temperature": temperature,
                "humidity": humidity,
                "alert_type": "temperature",
                "message": f"Temperature {temperature}°C is above 40°C",
            }

            producer.send(
                temperature_alerts_topic,
                key=str(sensor_id),
                value=alert
            )
            producer.flush()
            print(f"Sent temperature alert: {alert}")

        # Перевірка вологості
        if humidity is not None and (humidity > 80 or humidity < 20):
            condition = "above 80%" if humidity > 80 else "below 20%"
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "temperature": temperature,
                "humidity": humidity,
                "alert_type": "humidity",
                "message": f"Humidity {humidity}% is {condition}",
            }

            producer.send(
                humidity_alerts_topic,
                key=str(sensor_id),
                value=alert
            )
            producer.flush()
            print(f"Sent humidity alert: {alert}")

except KeyboardInterrupt:
    print("Stopped by user (Ctrl+C).")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()
    producer.close()
    print("Consumer and producer closed.")
