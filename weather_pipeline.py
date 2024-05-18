import threading
import time
from data_crawler import DataCrawler
from db_connector import DatabaseConnector
from data_processor import DataProcessor
from producer import send_data
from consumer import consum

class WeatherPipeline:
    def __init__(self):
        self.data_crawler = DataCrawler()
        self.db_connector = DatabaseConnector()
        self.data_processor = DataProcessor()
        self.spark_processor = SparkProcessor()
    
    def producer_thread(self, db=1):
        try:
            while True:
                for batch in self.data_crawler.fetch_data_in_batches(db=db):
                    send_data(batch, topic="weather_1" if db == 1 else "weather_2")
                    time.sleep(5)
                print("Data sent successfully")
                time.sleep(15 * 60)  # Sleep for 15 minutes
        except KeyboardInterrupt:
            print("Producer terminated.")
        except Exception as e:
            print(f'Error in producer in topic {db}: {str(e)}')
    
    def consumer_thread(self, db=1):
        try:
            if db == 1:
                mysql_conn = self.db_connector.connect_to_mysql()

                if mysql_conn:
                    self.db_connector.create_database_schema(mysql_conn)
                    self.db_connector.insert_location_data(mysql_conn)
                else:
                    print("Failed to connect to MySQL database.")

                while True:
                    for current, forecast in self.spark_processor.process_data(consum("weather_1")):
                        self.spark_processor.send_mysql(mysql_conn, current, table_name = "current")
                        self.spark_processor.send_mysql(mysql_conn, forecast, table_name = "daily")
                    print("Load data successfully")

            elif db == 2:
                postgresql_conn = self.db_connector.connect_to_postgresql()

                if postgresql_conn:
                    self.db_connector.create_database_schema(postgresql_conn, db="postgresql")
                    self.db_connector.insert_location_data(postgresql_conn, db="postgresql")
                else:
                    print("Failed to connect to PostgreSQL database.")
                while True:
                    for current, forecast in self.spark_processor.process_data(consum("weather_2")):
                        self.spark_processor.send_postgresql(postgresql_conn, current, table_name = "current")
                        self.spark_processor.send_postgresql(postgresql_conn, forecast, table_name = "daily")
                    print("Load data successfully")

        except KeyboardInterrupt:
            print("Consumer terminated.")
        except Exception as e:
            print(f'Error in consumer thread {db}: {str(e)}')

    def run(self):
        # Create separate threads for producer and consumer
        producer_thread_1 = threading.Thread(target=self.producer_thread, args=(1,))
        consumer_thread_1 = threading.Thread(target=self.consumer_thread, args=(1,))
        producer_thread_2 = threading.Thread(target=self.producer_thread, args=(2,))
        consumer_thread_2 = threading.Thread(target=self.consumer_thread, args=(2,))

        try:
            # Start the threads
            producer_thread_1.start()
            producer_thread_2.start()
            consumer_thread_1.start()
            consumer_thread_2.start()

            # Wait for the consumer thread to finish (producer thread runs indefinitely)
            consumer_thread_1.join()
            consumer_thread_2.join()
        except KeyboardInterrupt:
            print("Weather Pipeline terminated by user.")
        finally:
            # Stop the producer threads gracefully
            producer_thread_1.join()
            producer_thread_2.join()


pipeline = WeatherPipeline()
pipeline.run()
