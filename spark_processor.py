from pyspark.sql import SparkSession

class SparkProcessor:
    def __init__(self):
        self.spark = SparkSession.builder.appName("WeatherProcessor").getOrCreate()
    
    def consume(self, topic):
        data = self.spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", topic) \
            .option("group.id", "test-consumer-group") \
            .option("value.deserializer", lambda x: x.decode('utf-8')) \
            .load()
        
        return data

    def process_data(self, data):
        json_list = data.selectExpr("CAST(value AS STRING)") \
            .rdd.map(lambda x: x.value) \
            .collect()
        for obj in json_list:
            locationID = obj['id']
            current = obj['current']
            daily = obj['daily']
            forcast = []
            for i in range(7):
                forcast.append(
                    {locationID: locationID, "time": daily['time'][i], "temperature_2m_max": daily['temperature_2m_max'][i], "temperature_2m_min": daily['temperature_2m_min'][i], "apparent_temperature_max": daily['apparent_temperature_max'][i], "apparent_temperature_min": daily['apparent_temperature_min'][i], "sunrise": daily['sunrise'][i], "sunset": daily['sunset'][i]}
                )
            current['locationID'] = locationID
            current.remove("interval")
            yield current, forcast

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
    

    
