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
    
    def producer_thread(self):
        try:
            while True:
                for batch in self.data_crawler.fetch_data_in_batches():
                    send_data(batch)
                    time.sleep(5)
                print("Data sent successfully")
                time.sleep(15 * 60)  # Sleep for 15 minutes
        except KeyboardInterrupt:
            print("Producer terminated.")
        except Exception as e:
            print(f'Error in producer: {str(e)}')
    
    def consumer_thread(self):
        try:
            # Connect to MySQL database
            mysql_conn = self.db_connector.connect_to_mysql()
            if mysql_conn:
                # Create database schema
                self.db_connector.create_database_schema(mysql_conn)
                # Insert location data
                self.db_connector.insert_location_data(mysql_conn)
            else:
                print("Failed to connect to MySQL database.")

            while True:
                for data in consum():
                    self.data_processor.load_data_to_sqldb(mysql_conn, data)  
                print("Load data successfully")

        except KeyboardInterrupt:
            print("Consumer terminated.")
        except Exception as e:
            print(f'Error in consumer thread: {str(e)}')

    def run(self):
        # Create separate threads for producer and consumer
        producer_thread = threading.Thread(target=self.producer_thread)
        consumer_thread = threading.Thread(target=self.consumer_thread)

        try:
            # Start the threads
            producer_thread.start()
            consumer_thread.start()

            # Wait for the consumer thread to finish (producer thread runs indefinitely)
            consumer_thread.join()
        except KeyboardInterrupt:
            print("Weather Pipeline terminated by user.")
        finally:
            # Stop the producer thread gracefully
            producer_thread.join()

pipeline = WeatherPipeline()
pipeline.run()
