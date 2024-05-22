from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans


class DataProcessor:
    def __init__(self):
        pass

    def get_data(self):
        nocodb_url = "http://127.0.0.1:8085/api/v1/query"
        nocodb_key = 

        pass
    
    def cluster_data(self, data):
        spark = SparkSession.builder \
            .appName("Data Clustering") \
            .getOrCreate()

        df = spark.createDataFrame(data, ["longitude", "latitude", "address"])

        # Clutering
        kmeans = KMeans().setK(70).setSeed(1)
        model = kmeans.fit(df)
        
        
        # Return the clustered data
        return clustered_data