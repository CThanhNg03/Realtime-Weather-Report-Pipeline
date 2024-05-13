from kafka import KafkaConsumer
import json 

bootstrap_server = "localhost:9092"
topic = "weather"

def consum():
    consumer = KafkaConsumer(
        topic,
        group_id='test-consumer-group',
        bootstrap_servers=bootstrap_server,
        value_deserializer=lambda x: x.decode('utf-8')
    )

    try:
        # Iterate over messages in the topic
        for message in consumer:
            try:
                # Decode message value from bytes to string
                message_data = message.value

                # Yield the message data
                yield json.loads(message_data)

            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                continue

    except KeyboardInterrupt:
        print("Consumer terminated.")
    except Exception as e:
        print(f'Error in consumer: {str(e)}')
    finally:
        # Close the Kafka consumer
        consumer.close()
    