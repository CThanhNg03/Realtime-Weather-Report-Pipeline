from kafka import KafkaProducer
import json

bootstrap_servers = 'localhost:9092'
topic = 'weather'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

def send_data(data):
    try:
# Serialize the data to JSON
        serialized_data = json.dumps(data)
        
        # Convert the serialized data to bytes
        value_bytes = serialized_data.encode('utf-8')
        
        # Send the serialized data to Kafka
        producer.send(topic, value=value_bytes)
        producer.flush()
        # print("Data sent successfully")
    except Exception as e:
        print("Error sending data:", repr(e))
