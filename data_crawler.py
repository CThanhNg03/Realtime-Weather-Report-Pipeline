import pandas as pd
import requests
import json

class DataCrawler:
    def __init__(self):
        self.base_url = "https://api.open-meteo.com/v1/forecast"
        self.filepath = "geolocation.csv"
        self.df = pd.read_csv(self.filepath)
        self.df1 = self.df.iloc[:len(self.df)//2]
        self.df2 = self.df.iloc[len(self.df)//2:]

    def fetch_data_in_batches(self, db=1, batch_size=30, data_type="casual"):
        if db == 1:
            df = self.df1
        elif db == 2:
            df = self.df2
        total_places = len(df)

        for start in range(0, total_places, batch_size):
            end = min(start + batch_size, total_places)
            longitudes, latitudes = self.get_long_lat(start, end)
            crawled = self.get_data(longitudes, latitudes, data_type)

            # Add index to row
            for id, row in enumerate(crawled, start=start):
                row['id'] = id
                yield row

    def get_long_lat(self, start, end):
        longitudes = self.df['Longitude'][start:end].tolist()
        latitudes = self.df['Latitude'][start:end].tolist()
        return longitudes, latitudes

    def get_data(self, longitudes, latitudes, data_type):
        params = {
            'latitude': latitudes,
            'longitude': longitudes,
            "current": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "precipitation_probability", "precipitation", "rain", "showers", "snowfall", "snow_depth", "pressure_msl", "surface_pressure", "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high", "visibility", "evapotranspiration", "et0_fao_evapotranspiration", "vapour_pressure_deficit", "wind_speed_10m", "wind_speed_80m", "wind_speed_120m", "wind_speed_180m", "wind_direction_10m", "wind_direction_80m", "wind_direction_120m", "wind_direction_180m", "wind_gusts_10m", "temperature_80m", "temperature_120m", "temperature_180m", "soil_temperature_0cm", "soil_temperature_6cm", "soil_temperature_18cm", "soil_temperature_54cm", "soil_moisture_0_to_1cm", "soil_moisture_1_to_3cm", "soil_moisture_3_to_9cm", "soil_moisture_9_to_27cm", "soil_moisture_27_to_81cm", "uv_index", "uv_index_clear_sky", "is_day", "sunshine_duration"],
            "daily":["temperature_2m_max","temperature_2m_min","apparent_temperature_max","apparent_temperature_min","sunrise","sunset"],
            "timezone": "Asia/Bangkok",
        }
        if data_type == "casual":
            params["current"] = ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "precipitation_probability", "precipitation", "rain", "showers", "snowfall", "snow_depth", "cloud_cover", "visibility", "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m", "uv_index", "uv_index_clear_sky", "is_day", "sunshine_duration"]
        response = requests.get(self.base_url, params=params)
        if response.status_code == 200:
            data = json.loads(response.text)
            return data
        else:
            print("Error while crawl:", response.status_code)
            return None

# # Example usage:
# crawler = DataCrawler()
# for batch in crawler.fetch_data_in_batches(batch_size=10, data_type="casual"):
#     print(batch)  # Process each batch of data here
