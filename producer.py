from kafka import KafkaProducer

bootstrap_servers = 'localhost:9092'
topic = 'weather'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

def send_data(data):
    try:
        producer.send(topic, value=data)
        producer.flush()
        print("Data sent successfully")
    except Exception as e:
        print("Error sending data:", e)