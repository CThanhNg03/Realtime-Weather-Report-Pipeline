from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json
import json
from datetime import datetime

class SparkProcessor:
    def __init__(self):
        self.spark = SparkSession.builder.appName("WeatherProcessor") \
        .master("local[*]") \
        .config("spark.jars", "mysql-connector-java-8.0.30.jar") \
        .getOrCreate()
    
    def consume(self, topic):
        data = self.spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", topic) \
            .option("group.id", "test-consumer-group") \
            .option("value.deserializer", lambda x: x.decode('utf-8')) \
            .load()
        
        return data

    def process_data(self, data):
        parsed_data = data.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), "map<string, string>").alias("parsed_value"))

        exploded_data = parsed_data.select(explode("parsed_value").alias("id", "data"))
        if exploded_data.count() == 0:
            return  # Handle empty data

        location_row = exploded_data.filter(exploded_data["id"] == "id").collect()
        if not location_row:
            return  # Handle missing location data

        locationID = location_row[0]["data"]

        current_row = exploded_data.filter(exploded_data["id"] == "current").collect()
        daily_row = exploded_data.filter(exploded_data["id"] == "daily").collect()

        if not current_row or not daily_row:
            return  # Handle missing current or daily data

        current = json.loads(current_row[0]["data"])
        daily = json.loads(daily_row[0]["data"])
        
        forecast = []

        for i in range(7):
            forecast.append({
                "locationID": locationID,
                "time": daily['time'][i],
                "temperature_2m_max": daily['temperature_2m_max'][i],
                "temperature_2m_min": daily['temperature_2m_min'][i],
                "apparent_temperature_max": daily['apparent_temperature_max'][i],
                "apparent_temperature_min": daily['apparent_temperature_min'][i],
                "sunrise": datetime.strptime(daily['sunrise'][i], '%Y-%m-%dT%H:%M'),
                "sunset": datetime.strptime(daily['sunset'][i], '%Y-%m-%dT%H:%M')
            })

        current['time'] = datetime.strptime(current['time'], '%Y-%m-%dT%H:%M')
        current['locationID'] = locationID
        current.pop("interval", None)

        yield self.spark.createDataFrame([current]), self.spark.createDataFrame(forecast)


    def send_mysql(self, data, table_name="current"):

        # Load data to MySQL database
        data.write.format("jdbc").options(
            url="jdbc:mysql://35.198.235.20/weather_data",
            driver="com.mysql.jdbc.Driver",
            dbtable=table_name,
            user="root",
            password="123456a@"
        ).mode("append").save()

    def send_postgresql(self, data, table_name="current"):
        # Load data to PostgreSQL database
        data.write.format("jdbc").options(
            url="jdbc:postgresql://34.143.211.214/weather_data",
            driver="org.postgresql.Driver",
            dbtable=table_name,
            user="postgres",
            password="123456a@"
        ).mode("append").save()
    

    
