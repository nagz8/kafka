from kafka import KafkaProducer
import json
import time

# Configura el producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Topic al que se enviarán los mensajes
topic = 'labrafatopic'

# Enviar 5 mensajes de prueba
for i in range(5):
    data = {'mensaje': f'Hola Kafka {i}'}
    producer.send(topic, value=data)
    print(f"Enviado: {data}")
    time.sleep(1)  # Esperar 1 segundo entre mensajes

producer.flush()
