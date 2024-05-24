from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf, col, explode, max, avg, collect_list
import requests
import json
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, DecimalType, ArrayType, StringType, FloatType, BooleanType
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from geopy.geocoders import Nominatim
from pyspark.ml.stat import Correlation

def executeRestApi(verb, url, body, page):
    res = None
    token = "drKI1RM_aluVex4hzQci03vDyoQMM_5Xm7uC4XFb"
    headers = {
        "accept": "application/json",
        "xc-token": token
    }
    # Make API request, get response object back, create dataframe from above schema.
    try:
        if verb == "get":
            res = requests.get(f"{url}?limit=500&offset={page*500}", data=body, headers=headers)
        else:
            res = requests.post(f"{url}?limit=500&offset={page*500}", data=body, headers=headers)
    except Exception as e:
        return e
    if res is not None and res.status_code == 200:
        return {'list': json.loads(res.text)["list"]}
    return None

def getCount(url):
    token = "drKI1RM_aluVex4hzQci03vDyoQMM_5Xm7uC4XFb"
    headers = {
        "accept": "application/json",
        "xc-token": token
    }
    res = requests.get(f"{url}/count", headers=headers)
    return json.loads(res.text)["count"]

geolocator = Nominatim(user_agent="myGeocoder")

def get_address(latitude, longitude):
    try:
        location = geolocator.reverse((latitude, longitude))
        if location:
            address = location.address
            parts = address.split(",")
            result = ", ".join(parts[-2:-1]).strip()
            return result
    except Exception as e:
        return None
    return None

def get_geojson(row):
    coordinates = list(zip(row["longitudes"], row["latitudes"]))
    coordinates = [list(item) for item in coordinates]
    properties = {
        "name": row["centroid_address"],
        "temperature_2m_max": row["temperature_2m_max"],
        "temperature_2m_min": row["temperature_2m_min"],
        "apparent_temperature_max": row["apparent_temperature_max"],
        "apparent_temperature_min": row["apparent_temperature_min"]
    }
    feature = {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": coordinates
        },
        "properties": properties
    }

    return feature

class NocoProcessor: 
    def __init__(self):
        self.spark = SparkSession.builder.appName("NocoProcessor") \
                    .master("spark://10.10.28.50:7077") \
                    .getOrCreate()
        self.current = StructType([
            StructField("locationID", StringType()),
            StructField("time", StringType()),
            StructField("temperature_2m", StringType()),
            StructField("relative_humidity_2m", IntegerType()),
            StructField("dew_point_2m", StringType()),
            StructField("apparent_temperature", StringType()),
            StructField("precipitation_probability", IntegerType()),
            StructField("precipitation", StringType()),
            StructField("rain", StringType()),
            StructField("showers", StringType()),
            StructField("snowfall", StringType()),
            StructField("snow_depth", StringType()),
            StructField("cloud_cover", IntegerType()),
            StructField("visibility", StringType()),
            StructField("wind_speed_10m", StringType()),
            StructField("wind_direction_10m", IntegerType()),
            StructField("wind_gusts_10m", StringType()),
            StructField("uv_index", StringType()),
            StructField("uv_index_clear_sky", StringType()),
            StructField("is_day", IntegerType()),
            StructField("sunshine_duration", StringType())
        ])
        self.daily = StructType([
            StructField("id", IntegerType()),
            StructField("locationID", StringType()),
            StructField("time", StringType()),
            StructField("temperature_2m_min", StringType()),
            StructField("temperature_2m_max", StringType()),
            StructField("apparent_temperature_min", StringType()),
            StructField("apparent_temperature_max", StringType()),
            StructField("sunrise", StringType()),
            StructField("sunset", StringType()),
            StructField("time_created", StringType())
        ])
        self.location = StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("latitude", StringType()),
            StructField("longitude", StringType()),
            StructField("address", StringType())
        ])
        self.url = "http://localhost:8085/api/v2/tables/"
        self.token = "drKI1RM_aluVex4hzQci03vDyoQMM_5Xm7uC4XFb"
        self.current_1 = "m8xknwv434bmshs"
        self.current_2 = "m640s5ii47qu71s"
        self.daily_1 = "mkzhlrxen89llxt"
        self.daily_2 = "mkn391r4ydjttwv"
        self.location_1 = ""
        pass

    def fetch_api(self, table_name):
        switcher = {
            "current": [self.current_1, self.current],
            "current_2": [self.current_2, self.current],
            "daily": [self.daily_1, self.daily],
            "daily_2": [self.daily_2, self.daily],
            "location": [self.location_1, self.location]
        }
        url = self.url + switcher.get(table_name)[0] + "/records"
        
        schema = switcher.get(table_name)[1]
        res_schema = StructType([
            StructField("list", ArrayType(schema))
        ])
        udf_executeRestApi = udf(executeRestApi, res_schema)

        body = json.dumps({})
        requestRows = []
        RestApiRequestRow = Row("verb", "url", "body", "page")
        for page in range(0, getCount(url) // 500 + 1):
            requestRows.append(RestApiRequestRow("get", url, body, page))
        request_df = self.spark.createDataFrame(requestRows)
        result_df = request_df \
                    .withColumn("result", udf_executeRestApi(col("verb"), col("url"), col("body"), col("page")))
        df = result_df.select(explode(col("result.list")).alias("result")).select("result.*")
        if schema == self.current:
            df = df.withColumn("temperature_2m", col("temperature_2m").cast(FloatType())) \
                    .withColumn("relative_humidity_2m", col("relative_humidity_2m").cast(IntegerType())) \
                    .withColumn("dew_point_2m", col("dew_point_2m").cast(FloatType())) \
                    .withColumn("apparent_temperature", col("apparent_temperature").cast(FloatType())) \
                    .withColumn("precipitation_probability", col("precipitation_probability").cast(IntegerType())) \
                    .withColumn("precipitation", col("precipitation").cast(FloatType())) \
                    .withColumn("rain", col("rain").cast(FloatType())) \
                    .withColumn("showers", col("showers").cast(FloatType())) \
                    .withColumn("snowfall", col("snowfall").cast(FloatType())) \
                    .withColumn("snow_depth", col("snow_depth").cast(FloatType())) \
                    .withColumn("cloud_cover", col("cloud_cover").cast(IntegerType())) \
                    .withColumn("visibility", col("visibility").cast(FloatType())) \
                    .withColumn("wind_speed_10m", col("wind_speed_10m").cast(FloatType())) \
                    .withColumn("wind_direction_10m", col("wind_direction_10m").cast(IntegerType())) \
                    .withColumn("wind_gusts_10m", col("wind_gusts_10m").cast(FloatType())) \
                    .withColumn("uv_index", col("uv_index").cast(FloatType())) \
                    .withColumn("uv_index_clear_sky", col("uv_index_clear_sky").cast(FloatType())) \
                    .withColumn("is_day", col("is_day").cast(BooleanType())) \
                    .withColumn("sunshine_duration", col("sunshine_duration").cast(FloatType()))
        elif schema == self.daily:
            df = df.withColumn("temperature_2m_min", col("temperature_2m_min").cast(FloatType())) \
                    .withColumn("temperature_2m_max", col("temperature_2m_max").cast(FloatType())) \
                    .withColumn("apparent_temperature_min", col("apparent_temperature_min").cast(FloatType())) \
                    .withColumn("apparent_temperature_max", col("apparent_temperature_max").cast(FloatType())) \
                    .withColumn("time_created", col("time_created").cast(TimestampType())) \
                    .withColumn("time", col("time").cast(TimestampType()))
        else: 
            df = df.withColumn("latitude", col("latitude").cast(FloatType())) \
                    .withColumn("longitude", col("longitude").cast(FloatType()))
        return df

    def process_daily(self):
        daily = self.fetch_api("daily").union(self.fetch_api("daily_2"))
        daily = daily.groupBy("time", "locationID").agg(max("time_created").alias("newest_time_created"))
        location = self.fetch_api("location")
        location = location.withColumnRenamed("id", "locationID")
        daily = daily.join(location, "locationID", "inner")
        
        features = ["longitude", "latitude", "temperature_2m_max", "temperature_2m_min", "apparent_temperature_max", "apparent_temperature_min"]
        assembler = VectorAssembler(inputCols=features, outputCol="features")
        daily = assembler.transform(daily)

        kmeans = KMeans().setK(70).setSeed(1)
        model = kmeans.fit(daily)
        clustered = model.transform(daily)
        
        cluster_summary = clustered.groupBy("time", "prediction") \
            .agg(
                avg("temperature_2m_max").alias("temperature_2m_max"),
                avg("temperature_2m_min").alias("temperature_2m_min"),
                avg("apparent_temperature_max").alias("apparent_temperature_max"),
                avg("apparent_temperature_min").alias("apparent_temperature_min"),
                collect_list("longitude").alias("longitudes"),
                collect_list("latitude").alias("latitudes"),
                avg("longitude").alias("centroid_longitude"),
                avg("latitude").alias("centroid_latitude")
            )

        cluster_summary = cluster_summary.withColumn("centroid_address", get_address(col("centroid_latitude"), col("centroid_longitude")))

        geojson = cluster_summary.rdd.map(get_geojson)

        return geojson

    def process_current(self):
        current = self.fetch_api("current").union(self.fetch_api("current_2"))
        location = self.fetch_api("location").withColumnRenamed("id", "locationID")
        current = current.merge(location, on="locationID", how="inner")

        features = ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "precipitation_probability", "precipitation", "rain", "showers", "snowfall", "snow_depth", "cloud_cover", "visibility", "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m", "uv_index", "uv_index_clear_sky", "is_day", "sunshine_duration"]
        assembler = VectorAssembler(inputCols=features, outputCol="features")
        current = assembler.transform(current)
        kmeans = KMeans().setK(70).setSeed(1)
        model = kmeans.fit(daily)
        clustered = model.transform(daily)

        cluster_summary = clustered.groupBy("time", "prediction").agg(
            avg("temperature_2m").alias("avg_temperature_2m"),
            avg("relative_humidity_2m").alias("avg_relative_humidity_2m"),
            avg("dew_point_2m").alias("avg_dew_point_2m"),
            avg("apparent_temperature").alias("avg_apparent_temperature"),
            avg("precipitation_probability").alias("avg_precipitation_probability"),
            avg("precipitation").alias("avg_precipitation"),
            avg("rain").alias("avg_rain"),
            avg("showers").alias("avg_showers"),
            avg("snowfall").alias("avg_snowfall"),
            avg("snow_depth").alias("avg_snow_depth"),
            avg("cloud_cover").alias("avg_cloud_cover"),
            avg("visibility").alias("avg_visibility"),
            avg("wind_speed_10m").alias("avg_wind_speed_10m"),
            avg("wind_direction_10m").alias("avg_wind_direction_10m"),
            avg("wind_gusts_10m").alias("avg_wind_gusts_10m"),
            avg("uv_index").alias("avg_uv_index"),
            avg("uv_index_clear_sky").alias("avg_uv_index_clear_sky"),
            avg("is_day").alias("avg_is_day"),
            avg("sunshine_duration").alias("avg_sunshine_duration"),
            avg("longitude").alias("centroid_longitude"),
            avg("latitude").alias("centroid_latitude")
        )

        cluster_summary = cluster_summary.withColumn("centroid_address", get_address(col("centroid_latitude"), col("centroid_longitude")))

        features = ["avg_temperature_2m", "avg_relative_humidity_2m", "avg_dew_point_2m", "avg_apparent_temperature", "avg_precipitation_probability", "avg_precipitation", "avg_rain", "avg_showers", "avg_snowfall", "avg_snow_depth", "avg_cloud_cover", "avg_visibility", "avg_wind_speed_10m", "avg_wind_direction_10m", "avg_wind_gusts_10m", "avg_uv_index", "avg_uv_index_clear_sky", "avg_is_day", "avg_sunshine_duration"]
        assembler = VectorAssembler(inputCols=features, outputCol="features")
        cluster_summary = assembler.transform(cluster_summary)

        cluster_summary.withColumn("corr_matrix", Correlation.corr("features", "features"))

        corr_df = cluster_summary.select(
            "time", "locationID",
            expr("explode(matrix_to_rows(corr_matrix)) as (matrix_x, matrix_y, correlation)")
            )
        
        pass

test = NocoProcessor()
df = test.fetch_api("daily")
df.show()
