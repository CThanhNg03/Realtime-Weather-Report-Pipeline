import findspark
import threading
import time
from data_crawler import DataCrawler
from db_connector import DatabaseConnector
from producer import send_data
from consumer import consum
from spark_processor import SparkProcessor
import os

findspark.init()
# from data_processor import DataProcessor
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'

class WeatherPipeline:
    def __init__(self):
        self.data_crawler = DataCrawler()
        self.db_connector = DatabaseConnector()
        # self.data_processor = DataProcessor()
        self.spark_processor = SparkProcessor()
    
    def producer_thread(self, db=1):
        try:
            while True:
                for batch in self.data_crawler.fetch_data_in_batches(db=db):
                    send_data(batch, topic="weather_1" if db == 1 else "weather_2")
                    time.sleep(10)
                log_message = f"{time.strftime('%Y-%m-%d %H:%M:%S')} - INFO - Producer {db} sent successfully"
                print(log_message)
                time.sleep(45 * 60)  # Sleep for 45 minutes
        except Exception as e:
            log_message = f"{time.strftime('%Y-%m-%d %H:%M:%S')} - ERROR - Error in producer in topic {db}: {str(e)}"
            print(log_message)
    
    def process_and_send(self, batch_df, db=1):
        for current, forecast in self.spark_processor.process_data(batch_df):
            if current is None or forecast is None:
                continue 
            if db == 1:
                self.spark_processor.send_mysql(current, table_name="current")
                self.spark_processor.send_mysql(forecast, table_name="daily")
            elif db == 2:
                self.spark_processor.send_mysql(current, table_name="current_2")
                self.spark_processor.send_mysql(forecast, table_name="daily_2")
        log_message = f"{time.strftime('%Y-%m-%d %H:%M:%S')} - INFO - Consumer {db} load data successfully"
        print(log_message)


    def consumer_thread(self, db=1):
        try:
            mysql_conn = self.db_connector.connect_to_mysql()

            if mysql_conn:
                self.db_connector.create_database_schema(mysql_conn)
                self.db_connector.insert_location_data(mysql_conn)
            else:
                log_message = f"{time.strftime('%Y-%m-%d %H:%M:%S')} - ERROR - Consumer {db}: Failed to connect to MySQL database."
                print(log_message)
                return

            while True:
                data = self.spark_processor.consume("weather_1" if db == 1 else "weather_2")
                query = data.writeStream \
                    .foreachBatch(lambda batch_df, batch_id: self.process_and_send(batch_df, db)) \
                    .start()
                query.awaitTermination()
        except Exception as e:
            log_message = f"{time.strftime('%Y-%m-%d %H:%M:%S')} - ERROR - Error in consumer thread {db}: {str(e)}"
            print(log_message)

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
            time.sleep(60)
            consumer_thread_1.start()
            consumer_thread_2.start()

            # Wait for the consumer thread to finish (producer thread runs indefinitely)
            consumer_thread_1.join()
            consumer_thread_2.join()
        except KeyboardInterrupt:
            log_message = f"{time.strftime('%Y-%m-%d %H:%M:%S')} - INFO - Weather Pipeline terminated by user."
            print(log_message)
        finally:
            # Stop the producer threads gracefully
            producer_thread_1.join()
            producer_thread_2.join()


pipeline = WeatherPipeline()
pipeline.run()
