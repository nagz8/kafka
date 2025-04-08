from kafka import KafkaConsumer
import json

# Configura el consumer
consumer = KafkaConsumer(
    'labrafatopic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # desde el inicio
    enable_auto_commit=True,
    group_id='mi-grupo',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Esperando mensajes...")

for mensaje in consumer:
    print(f"Recibido: {mensaje.value}")
