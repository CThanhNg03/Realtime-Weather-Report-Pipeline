from pyspark.sql import SparkSession
import requests
import json
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, DecimalType

class NocoProcessor: 
    def __init__(self):
        self.spark = SparkSession.builder.appName("NocoProcessor").getOrCreate()
        self.current = StructType([
            StructField("locationID", IntegerType()),
            StructField("time", TimestampType()),
            StructField("temperature_2m", DecimalType(5, 2)),
            StructField("relative_humidity_2m", DecimalType(5, 2)),
            StructField("dew_point_2m", DecimalType(5, 2)),
            StructField("apparent_temperature", DecimalType(5, 2)),
            StructField("precipitation_probability", DecimalType(5, 2)),
            StructField("precipitation", DecimalType(5, 2)),
            StructField("rain", DecimalType(5, 2)),
            StructField("showers", DecimalType(5, 2)),
            StructField("snowfall", DecimalType(5, 2)),
            StructField("snow_depth", DecimalType(5, 2)),
            StructField("cloud_cover", DecimalType(5, 2)),
            StructField("visibility", IntegerType()),
            StructField("wind_speed_10m", DecimalType(5, 2)),
            StructField("wind_direction_10m", IntegerType()),
            StructField("wind_gusts_10m", DecimalType(5, 2)),
            StructField("uv_index", IntegerType()),
            StructField("uv_index_clear_sky", IntegerType()),
            StructField("is_day", IntegerType()),
            StructField("sunshine_duration", IntegerType())
        ])
        self.daily = StructType([
            StructField("locationID", IntegerType()),
            StructField("time", TimestampType()),
            StructField("temperature_2m_min", DecimalType(5, 2)),
            StructField("temperature_2m_max", DecimalType(5, 2)),
            StructField("precipitation_probability", DecimalType(5, 2)),
            StructField("precipitation", DecimalType(5, 2)),
            StructField("rain", DecimalType(5, 2)),
            StructField("showers", DecimalType(5, 2)),
            StructField("snowfall", DecimalType(5, 2)),
            StructField("snow_depth", DecimalType(5, 2)),
            StructField("cloud_cover", DecimalType(5, 2)),
            StructField("wind_speed_10m", DecimalType(5, 2)),
            StructField("wind_direction_10m", IntegerType()),
            StructField("wind_gusts_10m", DecimalType(5, 2)),
            StructField("uv_index", IntegerType()),
            StructField("uv_index_clear_sky", IntegerType()),
            StructField("sunshine_duration", IntegerType())
        ])
        self.url = "http://localhost:8085/api/v2/tables/"
        self.token = "drKI1RM_aluVex4hzQci03vDyoQMM_5Xm7uC4XFb"
        self.current2 = "myoa15cib39yazq"
        self.daily2 = "mlc087y9x0hopx6"
        pass

    def fetch_data(self, db=1, table_name="current"):
        if db = 1:
            url = "jdbc:mysql://35.198.235.20/weather_data"
            driver = "com.mysql.jdbc.Driver"
            user = "root"
            password = "123456a@"
        elif db = 2:
            url = "jdbc:postgresql://34.143.211.214/weather_data"
            driver = "org.postgresql.Driver"
            user="postgres"
            password="123456a@"

        df = spark.read.format("jdbc").options(
            url=url,
            driver=driver,
            dbtable=f"(select * from {table_name} where time = (select max(time) from {table_name})) as subquery",
            user=user,
            password=password
        ).load()
        return df

    def load_to_noco(self, db=1, table_name="current"):
        data = self.fetch_data(db, table_name).toJSON().collect()
        if table_name == "current":
            schema = self.current
            table = self.current2
        elif table_name == "daily":
            schema = self.daily
            table = self.daily2
        url = f'{self.url}{table}/records'
        
        # Iterate over the rows of the DataFrame
        for row in data.collect():            
            # Prepare the request body
            body = row.asDict()
            
            # Convert the body to JSON string
            json_body = json.dumps(body)
            
            # Make the POST request to the NocoDB API
            response = requests.post(url, headers=headers, data=json_body)
            
            # Handle the response as needed
            if response is not None:
                # Process the response
                pass
        
        

    